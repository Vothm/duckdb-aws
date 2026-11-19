#pragma once

#include "duckdb.hpp"

namespace duckdb {
class ExtensionLoader;

struct AthenaFunctions {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
