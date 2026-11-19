#pragma once

#include "duckdb.hpp"

namespace duckdb {
class ExtensionLoader;

struct AthenaResource {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
