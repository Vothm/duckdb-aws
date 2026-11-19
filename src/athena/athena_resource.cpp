#include "athena/athena_resource.hpp"

#include "arn.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_resource_type_registry.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

namespace {

constexpr const char *TYPE_NAME = "aws.athena.workgroup";
constexpr const char *FUNCTION_PREFIX = "__aws__athena__workgroup";

struct AdapterBindData : public TableFunctionData {
	explicit AdapterBindData(const Value &input_p) : input(input_p) {
	}
	Value input; // params (create/list) or handle (status)
};

struct AdapterState : public GlobalTableFunctionState {
	unique_ptr<QueryResult> result;
	idx_t cursor = 0;
	bool ran = false;
};

unique_ptr<GlobalTableFunctionState> AdapterInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<AdapterState>();
}

//! Read one key out of a params/handle MAP; "" when absent or NULL.
string MapLookup(const Value &map_value, const string &key) {
	if (map_value.IsNull()) {
		return string();
	}
	for (auto &entry : MapValue::GetChildren(map_value)) {
		auto &kv = StructValue::GetChildren(entry);
		if (!kv[0].IsNull() && StringValue::Get(kv[0]) == key) {
			return kv[1].IsNull() ? string() : StringValue::Get(kv[1]);
		}
	}
	return string();
}

unique_ptr<QueryResult> RunSQL(ClientContext &context, const string &sql, const string &callback) {
	Connection con(DatabaseInstance::GetDatabase(context));
	auto res = con.Query(sql);
	if (res->HasError()) {
		throw IOException("%s failed: %s", callback, res->GetError());
	}
	return res;
}

void EmitRows(AdapterState &state, DataChunk &output) {
	idx_t total = state.result->RowCount();
	idx_t to_emit = MinValue<idx_t>(total - state.cursor, STANDARD_VECTOR_SIZE);
	auto rows = state.result->Collection().GetRows();
	for (idx_t row_idx = 0; row_idx < to_emit; row_idx++) {
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			output.data[col].Append(rows.GetValue(col, state.cursor + row_idx));
		}
	}
	output.CheckCardinality(to_emit);
	state.cursor += to_emit;
}

unique_ptr<FunctionData> AdapterBind(TableFunctionBindInput &input) {
	return make_uniq<AdapterBindData>(input.inputs[0]);
}

//===--------------------------------------------------------------------===//
// list(params MAP) -> TABLE(handle MAP, name VARCHAR, reference VARCHAR, state VARCHAR)
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> ListBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                  vector<Identifier> &names) {
	names = {"handle", "name", "reference", "state"};
	return_types = {LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR};
	return AdapterBind(input);
}

void ListFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<AdapterState>();
	if (!state.ran) {
		state.result = RunSQL(context,
		                      "SELECT MAP {'arn': arn} AS handle, workgroup_name AS name, arn AS reference, "
		                      "CASE WHEN lower(w.state) = 'enabled' THEN 'ready' ELSE lower(w.state) END AS state "
		                      "FROM athena_workgroups() w WHERE error IS NULL ORDER BY region, name",
		                      string(TYPE_NAME) + " list");
		state.ran = true;
	}
	EmitRows(state, output);
}

//===--------------------------------------------------------------------===//
// status(handle MAP) -> TABLE(state VARCHAR, result MAP)
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> StatusBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                    vector<Identifier> &names) {
	names = {"state", "result"};
	return_types = {LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)};
	return AdapterBind(input);
}

void StatusFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<AdapterState>();
	auto &bind = data_p.bind_data->Cast<AdapterBindData>();
	if (!state.ran) {
		auto callback = string(TYPE_NAME) + " status";
		auto arn_string = MapLookup(bind.input, "arn");
		auto arn = ParsedArn::IsArn(arn_string) ? ParsedArn::Parse(arn_string) : ParsedArn();
		const string resource_prefix = "workgroup/";
		auto name = StringUtil::StartsWith(arn.resource, resource_prefix) ? arn.resource.substr(resource_prefix.size())
		                                                                  : string();
		if (arn.service != "athena" || arn.region.empty() || arn.account_id.empty() || name.empty()) {
			throw InvalidInputException("%s: expected an ARN of the form "
			                            "'arn:<partition>:athena:<region>:<account>:workgroup/<name>', got '%s'",
			                            callback, arn_string);
		}
		auto secret = MapLookup(bind.input, "secret");
		auto secret_sql = secret.empty() ? string() : Value(secret).ToSQLString();
		auto sql = StringUtil::Format(
		    "SELECT 'ready' AS state, MAP {'uri': arn, 'attached_db_type': 'arn'%s} AS result, "
		    "arn FROM athena_workgroups(region := %s, workgroup := %s%s)",
		    secret.empty() ? string() : ", 'secret': " + secret_sql, Value(StringUtil::Lower(arn.region)).ToSQLString(),
		    Value(name).ToSQLString(), secret.empty() ? string() : ", secret := " + secret_sql);
		state.result = RunSQL(context, sql, callback);
		if (state.result->RowCount() == 0) {
			throw IOException("%s: no workgroup with ARN '%s' in region '%s'", callback, arn.raw, arn.region);
		}
		auto seen = state.result->Collection().GetRows().GetValue(2, 0).ToString();
		if (seen != arn.raw) {
			throw InvalidInputException("%s: '%s' is not this workgroup's ARN as the credentials see it, '%s' (another "
			                            "account or partition, or a different spelling)",
			                            callback, arn.raw, seen);
		}
		state.ran = true;
	}
	EmitRows(state, output);
}

unique_ptr<FunctionData> CreateBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                    vector<Identifier> &names) {
	names = {"handle"};
	return_types = {LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)};
	return AdapterBind(input);
}

void CreateFun(ClientContext &, TableFunctionInput &, DataChunk &) {
	throw NotImplementedException(
	    "%s create: this extension does not create Athena workgroups; create it in AWS, then "
	    "find it with SHOW ALL EXTERNAL RESOURCES WITH TYPE '%s' and attach it by the reference listed there, or "
	    "REGISTER EXTERNAL RESOURCE '%s' AS <name> FROM MAP {'arn': '<reference>'}",
	    TYPE_NAME, TYPE_NAME, TYPE_NAME);
}

} // namespace

void AthenaResource::Register(ExtensionLoader &loader) {
	ExternalResourceType resource_type;
	resource_type.name = TYPE_NAME;
	resource_type.kind = "catalog";
	resource_type.create_function = string(FUNCTION_PREFIX) + "__create";
	resource_type.status_function = string(FUNCTION_PREFIX) + "__status";
	resource_type.list_function = string(FUNCTION_PREFIX) + "__list";
	resource_type.origin = "extension";

	const auto map_vv = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	loader.RegisterFunction(
	    TableFunction(Identifier(resource_type.create_function), {map_vv}, CreateFun, CreateBind, AdapterInit));
	loader.RegisterFunction(
	    TableFunction(Identifier(resource_type.status_function), {map_vv}, StatusFun, StatusBind, AdapterInit));
	loader.RegisterFunction(
	    TableFunction(Identifier(resource_type.list_function), {map_vv}, ListFun, ListBind, AdapterInit));
	ExternalResourceTypeRegistry::Get(loader.GetDatabaseInstance()).Add(std::move(resource_type));
}

} // namespace duckdb
