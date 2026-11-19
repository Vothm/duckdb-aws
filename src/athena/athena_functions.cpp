#include "athena/athena_functions.hpp"

#include "arn.hpp"
#include "aws_client.hpp"
#include "aws_http_client.hpp"
#include "utils/utils.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <aws/athena/AthenaClient.h>
#include <aws/athena/model/GetWorkGroupRequest.h>
#include <aws/athena/model/ListWorkGroupsRequest.h>
#include <aws/athena/model/WorkGroupState.h>
#include <aws/sts/STSClient.h>
#include <aws/sts/model/GetCallerIdentityRequest.h>

namespace duckdb {

namespace {

using CredentialsProvider = std::shared_ptr<Aws::Auth::AWSCredentialsProvider>;
using Rows = vector<vector<Value>>;

string FromAws(const Aws::String &s) {
	return {s.data(), s.size()};
}

Value StringOrNull(bool set, const Aws::String &s) {
	return set ? Value(FromAws(s)) : Value(LogicalType::VARCHAR);
}

template <class OUTCOME>
[[noreturn]] void ThrowAwsError(const char *call, const string &region, const OUTCOME &outcome) {
	const auto &error = outcome.GetError();
	throw IOException("%s failed in region '%s': %s - %s", call, region, FromAws(error.GetExceptionName()),
	                  FromAws(error.GetMessage()));
}

Aws::Client::ClientConfiguration ConfigFor(const string &region) {
	auto config = BuildClientConfigWithCa();
	config.region = region.c_str();
	return config;
}

ParsedArn CallerIdentity(const CredentialsProvider &provider, const string &region) {
	Aws::STS::STSClient client(provider, ConfigFor(region));
	auto outcome = client.GetCallerIdentity(Aws::STS::Model::GetCallerIdentityRequest());
	if (!outcome.IsSuccess()) {
		ThrowAwsError("STS GetCallerIdentity", region, outcome);
	}
	return ParsedArn::Parse(FromAws(outcome.GetResult().GetArn()));
}

template <class WORKGROUP>
void AddRow(Rows &rows, const ParsedArn &identity, const string &region, const WORKGROUP &workgroup,
            const Aws::Athena::Model::EngineVersion &engine) {
	auto name = FromAws(workgroup.GetName());
	auto state = Aws::Athena::Model::WorkGroupStateMapper::GetNameForWorkGroupState(workgroup.GetState());
	rows.push_back(
	    {Value(region), Value(name),
	     Value("arn:" + identity.partition + ":athena:" + region + ":" + identity.account_id + ":workgroup/" + name),
	     StringOrNull(workgroup.StateHasBeenSet(), state),
	     StringOrNull(engine.EffectiveEngineVersionHasBeenSet(), engine.GetEffectiveEngineVersion()),
	     Value(LogicalType::VARCHAR)});
}

void ListRegion(const CredentialsProvider &provider, const string &region, const string &workgroup, Rows &rows) {
	auto identity = CallerIdentity(provider, region);
	Aws::Athena::AthenaClient client(provider, ConfigFor(region));
	if (!workgroup.empty()) {
		Aws::Athena::Model::GetWorkGroupRequest request;
		request.SetWorkGroup(workgroup.c_str());
		auto outcome = client.GetWorkGroup(request);
		if (!outcome.IsSuccess()) {
			ThrowAwsError("Athena GetWorkGroup", region, outcome);
		}
		const auto &found = outcome.GetResult().GetWorkGroup();
		AddRow(rows, identity, region, found, found.GetConfiguration().GetEngineVersion());
		return;
	}
	Aws::Athena::Model::ListWorkGroupsRequest request;
	Aws::String token;
	do {
		if (!token.empty()) {
			request.SetNextToken(token);
		}
		auto outcome = client.ListWorkGroups(request);
		if (!outcome.IsSuccess()) {
			ThrowAwsError("Athena ListWorkGroups", region, outcome);
		}
		for (const auto &summary : outcome.GetResult().GetWorkGroups()) {
			AddRow(rows, identity, region, summary, summary.GetEngineVersion());
		}
		token = outcome.GetResult().GetNextToken();
	} while (!token.empty());
}

struct ListBindData : public TableFunctionData {
	string region;
	string secret_name;
	string workgroup;
};

struct ListState : public GlobalTableFunctionState {
	Rows rows;
	idx_t cursor = 0;
	bool ran = false;
};

unique_ptr<FunctionData> ListBind(ClientContext &, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                  vector<Identifier> &names) {
	auto result = make_uniq<ListBindData>();
	for (auto &np : input.named_parameters) {
		auto key = StringUtil::Lower(np.first.GetIdentifierName());
		if (np.second.IsNull() || StringValue::Get(np.second).empty()) {
			throw InvalidInputException("athena_workgroups: %s must not be NULL or empty", key);
		}
		if (key == "region") {
			result->region = StringValue::Get(np.second);
		} else if (key == "workgroup") {
			result->workgroup = StringValue::Get(np.second);
		} else {
			result->secret_name = StringValue::Get(np.second);
		}
	}
	for (auto &column : {"region", "workgroup_name", "arn", "state", "engine_version", "error"}) {
		names.emplace_back(column);
		return_types.emplace_back(LogicalType::VARCHAR);
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> ListInit(ClientContext &context, TableFunctionInitInput &) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("athena_workgroups: listing Athena workgroups is disabled through configuration");
	}
	return make_uniq<ListState>();
}

struct ListRegionTask : public BaseExecutorTask {
	ListRegionTask(TaskExecutor &executor, DatabaseInstance &db_p, const ListBindData &data_p,
	               CredentialsProvider provider_p, string region_p, Rows &slot_p)
	    : BaseExecutorTask(executor), db(db_p), data(data_p), provider(std::move(provider_p)),
	      region(std::move(region_p)), slot(slot_p) {
	}
	void ExecuteTask() override {
		// Scheduler threads do not inherit the caller's AwsInstanceBinding
		AwsInstanceBinding aws_binding(db);
		try {
			ListRegion(provider, region, data.workgroup, slot);
		} catch (std::exception &ex) {
			EmitError(ErrorData(ex).RawMessage());
		} catch (...) {
			EmitError("unknown error");
		}
	}
	void EmitError(const string &message) {
		slot.clear();
		slot.push_back({Value(region), Value(LogicalType::VARCHAR), Value(LogicalType::VARCHAR),
		                Value(LogicalType::VARCHAR), Value(LogicalType::VARCHAR), Value(message)});
	}
	DatabaseInstance &db;
	const ListBindData &data;
	CredentialsProvider provider;
	string region;
	Rows &slot;
};

void ListFun(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ListBindData>();
	auto &state = data_p.global_state->Cast<ListState>();
	if (!state.ran) {
		state.ran = true;
		auto &db = DatabaseInstance::GetDatabase(context);
		AwsInstanceBinding aws_binding(db);
		auto entry = FindAwsSecret(context, data.secret_name);
		auto provider = CredentialsProviderFromSecret(entry->secret->Cast<KeyValueSecret>(), "Athena");
		if (!data.region.empty()) {
			ListRegion(provider, data.region, data.workgroup, state.rows);
		} else {
			const auto &regions = GetDefaultAwsRegions();
			// Fixed-size, so the tasks' references into it stay valid
			vector<Rows> slots(regions.size());
			TaskExecutor executor(context);
			for (idx_t i = 0; i < regions.size(); i++) {
				executor.ScheduleTask(make_uniq<ListRegionTask>(executor, db, data, provider, regions[i], slots[i]));
			}
			executor.WorkOnTasks();
			for (auto &slot : slots) {
				for (auto &row : slot) {
					state.rows.push_back(std::move(row));
				}
			}
		}
	}
	auto count = MinValue<idx_t>(state.rows.size() - state.cursor, STANDARD_VECTOR_SIZE);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto &row = state.rows[state.cursor + row_idx];
		for (idx_t col = 0; col < output.ColumnCount(); col++) {
			output.data[col].Append(row[col]);
		}
	}
	output.CheckCardinality(count);
	state.cursor += count;
}

} // namespace

void AthenaFunctions::Register(ExtensionLoader &loader) {
	const auto options = [](TypedKwargs &kwargs) {
		kwargs.Add("region", LogicalType::VARCHAR)
		    .Add("secret", LogicalType::VARCHAR)
		    .Add("workgroup", LogicalType::VARCHAR);
	};
	TableFunction function("athena_workgroups", FunctionSignature().WithTypedKwargs("options", options), ListFun,
	                       ListBind, ListInit);
	loader.RegisterFunction(Documented(
	    function,
	    "Lists the Athena workgroups (ListWorkGroups) with their ARN, state and engine version; `workgroup` names one "
	    "and looks it up instead (GetWorkGroup) in each region listed. `region` lists one region and throws its error; "
	    "without it every default region is listed in parallel, and a region that cannot be listed becomes one row "
	    "with `error` set.",
	    "FROM athena_workgroups(region := 'us-east-1')", "athena"));
}

} // namespace duckdb
