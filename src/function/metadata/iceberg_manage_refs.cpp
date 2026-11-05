//! Branch and tag management, and publication of a branch (Spark's create/drop branch/tag DDL and the
//! fast_forward and set_current_snapshot procedures). Each call stages its updates and requirements in
//! the transaction; the refs change in one atomic catalog commit when the transaction commits.

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

enum class IcebergRefOperation : uint8_t {
	CREATE_BRANCH,
	CREATE_TAG,
	DROP_BRANCH,
	DROP_TAG,
	FAST_FORWARD,
	SET_CURRENT
};

struct IcebergManageRefsBindData : public TableFunctionData {
	IcebergRefOperation operation;
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	string ref_name;
	//! fast_forward: the ref whose snapshot is published; set_current_snapshot: the optional source ref
	string source_ref;
	optional<int64_t> snapshot_id;
	IcebergSnapshotRefRetention retention;
};

struct IcebergManageRefsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergManageRefsGlobalState>();
	}

	bool finished = false;
};

const char *FunctionName(IcebergRefOperation operation) {
	switch (operation) {
	case IcebergRefOperation::CREATE_BRANCH:
		return "iceberg_create_branch";
	case IcebergRefOperation::CREATE_TAG:
		return "iceberg_create_tag";
	case IcebergRefOperation::DROP_BRANCH:
		return "iceberg_drop_branch";
	case IcebergRefOperation::DROP_TAG:
		return "iceberg_drop_tag";
	case IcebergRefOperation::FAST_FORWARD:
		return "iceberg_fast_forward";
	case IcebergRefOperation::SET_CURRENT:
		return "iceberg_set_current_snapshot";
	default:
		throw InternalException("Unknown Iceberg ref operation");
	}
}

optional_ptr<IcebergTableSchemaVersion> BindIcebergTable(ClientContext &context, TableFunctionBindInput &input,
                                                         const char *function_name) {
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("%s: table_name cannot be NULL", function_name);
	}
	if (!input.binder) {
		//! Without a binder the modification cannot be registered, which would bypass read-only and
		//! transaction checks, so refuse instead.
		throw InvalidInputException("%s can only be called from a query", function_name);
	}
	auto input_string = input.inputs[0].ToString();
	auto qualified_name = QualifiedName::ParseComponents(input_string);
	//! Only probe local paths: a URL ('s3://...') would make a network request for what is a table name.
	if (qualified_name.size() == 1 && input_string.find("://") == string::npos) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (fs.DirectoryExists(input_string) || fs.FileExists(input_string)) {
			throw InvalidInputException("Cannot call %s() on a file/directory", function_name);
		}
	}
	auto entry = IcebergUtils::GetTableEntry(context, input_string);
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	if (catalog_entry.catalog.GetCatalogType() != "iceberg" || entry->type != CatalogType::TABLE_ENTRY) {
		throw InvalidInputException("Cannot call %s on non-iceberg table", function_name);
	}
	DatabaseModificationType modification;
	modification |= DatabaseModificationType::ALTER_TABLE;
	input.binder->GetStatementProperties().RegisterDBModify(entry->ParentCatalog(), context, modification);
	return entry->Cast<IcebergTableSchemaVersion>();
}

string GetRefNameArgument(const Value &value, const char *function_name, const char *argument) {
	if (value.IsNull()) {
		throw InvalidInputException("%s: %s cannot be NULL", function_name, argument);
	}
	auto result = StringValue::Get(value);
	if (result.empty()) {
		throw InvalidInputException("%s: %s cannot be empty", function_name, argument);
	}
	return result;
}

//! Snapshot ids are exact integers: refuse fractional or other types instead of silently rounding.
int64_t GetSnapshotIdValue(const Value &value, const char *function_name, const char *argument) {
	if (value.IsNull()) {
		throw InvalidInputException("%s: '%s' cannot be NULL", function_name, argument);
	}
	if (!value.type().IsIntegral()) {
		throw InvalidInputException("%s: '%s' must be an integer, got %s", function_name, argument,
		                            value.type().ToString());
	}
	auto result = value.DefaultTryCastAs(LogicalType::BIGINT, nullptr, true);
	if (!result || result->IsNull()) {
		throw InvalidInputException("%s: '%s' is out of range for a snapshot id", function_name, argument);
	}
	return result->GetValue<int64_t>();
}

int64_t GetRetentionValue(const Value &value, const char *function_name, const string &option) {
	if (value.IsNull()) {
		throw InvalidInputException("%s: '%s' cannot be NULL", function_name, option);
	}
	auto result = value.DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
	if (result <= 0) {
		throw InvalidInputException("%s: '%s' must be > 0, got %lld", function_name, option, result);
	}
	return result;
}

void ParseCreateOptions(TableFunctionBindInput &input, IcebergManageRefsBindData &bind_data) {
	auto function_name = FunctionName(bind_data.operation);
	const bool is_tag = bind_data.operation == IcebergRefOperation::CREATE_TAG;
	for (auto &kv : input.named_parameters) {
		auto option = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &value = kv.second;
		if (option == "snapshot_id") {
			bind_data.snapshot_id = GetSnapshotIdValue(value, function_name, "snapshot_id");
		} else if (option == "max_ref_age_ms") {
			bind_data.retention.max_ref_age_ms = GetRetentionValue(value, function_name, option);
		} else if (option == "max_snapshot_age_ms" && !is_tag) {
			bind_data.retention.max_snapshot_age_ms = GetRetentionValue(value, function_name, option);
		} else if (option == "min_snapshots_to_keep" && !is_tag) {
			auto count = GetRetentionValue(value, function_name, option);
			if (count > NumericLimits<int32_t>::Maximum()) {
				throw InvalidInputException("%s: 'min_snapshots_to_keep' is too large", function_name);
			}
			bind_data.retention.min_snapshots_to_keep = static_cast<int32_t>(count);
		} else {
			throw InvalidInputException("%s: unsupported option '%s'", function_name, option);
		}
	}
}

unique_ptr<FunctionData> BindManageRefs(IcebergRefOperation operation, ClientContext &context,
                                        TableFunctionBindInput &input, vector<LogicalType> &return_types,
                                        vector<Identifier> &names) {
	auto result = make_uniq<IcebergManageRefsBindData>();
	result->operation = operation;
	auto function_name = FunctionName(operation);
	result->iceberg_table = BindIcebergTable(context, input, function_name);

	switch (operation) {
	case IcebergRefOperation::CREATE_BRANCH:
	case IcebergRefOperation::CREATE_TAG:
		result->ref_name = GetRefNameArgument(input.inputs[1], function_name, "ref_name");
		ParseCreateOptions(input, *result);
		names = {"ref_name", "ref_type", "snapshot_id"};
		return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
		break;
	case IcebergRefOperation::DROP_BRANCH:
	case IcebergRefOperation::DROP_TAG:
		result->ref_name = GetRefNameArgument(input.inputs[1], function_name, "ref_name");
		names = {"ref_name", "ref_type", "snapshot_id"};
		return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
		break;
	case IcebergRefOperation::FAST_FORWARD:
		result->ref_name = GetRefNameArgument(input.inputs[1], function_name, "branch");
		result->source_ref = GetRefNameArgument(input.inputs[2], function_name, "to");
		names = {"branch_updated", "previous_ref", "updated_ref"};
		return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
		break;
	case IcebergRefOperation::SET_CURRENT: {
		auto &target = input.inputs[1];
		if (target.IsNull()) {
			throw InvalidInputException("%s: snapshot_id or ref cannot be NULL", function_name);
		}
		if (target.type().id() == LogicalTypeId::VARCHAR) {
			result->source_ref = GetRefNameArgument(target, function_name, "ref");
		} else {
			result->snapshot_id = GetSnapshotIdValue(target, function_name, "snapshot_id");
		}
		names = {"previous_snapshot_id", "current_snapshot_id"};
		return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
		break;
	}
	default:
		throw InternalException("Unknown Iceberg ref operation");
	}
	return std::move(result);
}

template <IcebergRefOperation OPERATION>
unique_ptr<FunctionData> ManageRefsBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	return BindManageRefs(OPERATION, context, input, return_types, names);
}

void SetOptionalBigint(Vector &vector, idx_t row, const optional<int64_t> &value) {
	if (value) {
		FlatVector::GetDataMutable<int64_t>(vector)[row] = *value;
	} else {
		FlatVector::SetNull(vector, row, true);
	}
}

void ManageRefsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergManageRefsBindData>();
	auto &global_state = data.global_state->Cast<IcebergManageRefsGlobalState>();
	if (!bind_data.iceberg_table || global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}

	auto &table_info = bind_data.iceberg_table->table_info;
	auto &iceberg_transaction = IcebergTransaction::Get(context, bind_data.iceberg_table->catalog);
	ApplyTableUpdate(table_info, iceberg_transaction, [&](IcebergTable &tbl) {
		auto &transaction_data = tbl.GetOrCreateTransactionData(iceberg_transaction);
		switch (bind_data.operation) {
		case IcebergRefOperation::CREATE_BRANCH:
		case IcebergRefOperation::CREATE_TAG: {
			auto is_tag = bind_data.operation == IcebergRefOperation::CREATE_TAG;
			auto snapshot_id = transaction_data.TableCreateRef(bind_data.ref_name,
			                                                   is_tag ? IcebergRefType::TAG : IcebergRefType::BRANCH,
			                                                   bind_data.snapshot_id, bind_data.retention);
			output.SetValue(0, 0, Value(bind_data.ref_name));
			output.SetValue(1, 0, Value(is_tag ? "tag" : "branch"));
			output.SetValue(2, 0, Value::BIGINT(snapshot_id));
			break;
		}
		case IcebergRefOperation::DROP_BRANCH:
		case IcebergRefOperation::DROP_TAG: {
			auto is_tag = bind_data.operation == IcebergRefOperation::DROP_TAG;
			auto snapshot_id = transaction_data.TableDropRef(bind_data.ref_name,
			                                                 is_tag ? IcebergRefType::TAG : IcebergRefType::BRANCH);
			output.SetValue(0, 0, Value(bind_data.ref_name));
			output.SetValue(1, 0, Value(is_tag ? "tag" : "branch"));
			output.SetValue(2, 0, Value::BIGINT(snapshot_id));
			break;
		}
		case IcebergRefOperation::FAST_FORWARD: {
			auto move = transaction_data.TableFastForward(bind_data.ref_name, bind_data.source_ref);
			output.SetValue(0, 0, Value(bind_data.ref_name));
			SetOptionalBigint(output.data[1], 0, move.previous_snapshot_id);
			output.SetValue(2, 0, Value::BIGINT(move.updated_snapshot_id));
			break;
		}
		case IcebergRefOperation::SET_CURRENT: {
			auto move = transaction_data.TableSetCurrentSnapshot(bind_data.snapshot_id, bind_data.source_ref);
			SetOptionalBigint(output.data[0], 0, move.previous_snapshot_id);
			output.SetValue(1, 0, Value::BIGINT(move.updated_snapshot_id));
			break;
		}
		default:
			throw InternalException("Unknown Iceberg ref operation");
		}
	});
	output.SetChildCardinality(1);
	global_state.finished = true;
}

template <IcebergRefOperation OPERATION>
TableFunction MakeFunction(FunctionSignature signature) {
	return TableFunction(std::move(signature), ManageRefsFunction, ManageRefsBind<OPERATION>,
	                     IcebergManageRefsGlobalState::Init);
}

template <IcebergRefOperation OPERATION>
TableFunctionSet MakeCreateRefFunctionSet() {
	TableFunctionSet function_set(FunctionName(OPERATION));
	auto fun = MakeFunction<OPERATION>(FunctionSignature()
	                                       .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                                       .AddPositionalOnly("ref_name", LogicalType::VARCHAR));
	fun.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		//! ANY so that BIGINT and the UBIGINT ids of iceberg_snapshots are both accepted.
		options.Add("snapshot_id", LogicalType::ANY).Add("max_ref_age_ms", LogicalType::BIGINT);
		if (OPERATION == IcebergRefOperation::CREATE_BRANCH) {
			options.Add("max_snapshot_age_ms", LogicalType::BIGINT).Add("min_snapshots_to_keep", LogicalType::BIGINT);
		}
	});
	function_set.AddFunction(fun);
	return function_set;
}

template <IcebergRefOperation OPERATION>
TableFunctionSet MakeDropRefFunctionSet() {
	TableFunctionSet function_set(FunctionName(OPERATION));
	function_set.AddFunction(MakeFunction<OPERATION>(FunctionSignature()
	                                                     .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                                                     .AddPositionalOnly("ref_name", LogicalType::VARCHAR)));
	return function_set;
}

} // namespace

vector<TableFunctionSet> IcebergFunctions::GetIcebergManageRefsFunctions() {
	vector<TableFunctionSet> result;
	result.push_back(MakeCreateRefFunctionSet<IcebergRefOperation::CREATE_BRANCH>());
	result.push_back(MakeCreateRefFunctionSet<IcebergRefOperation::CREATE_TAG>());
	result.push_back(MakeDropRefFunctionSet<IcebergRefOperation::DROP_BRANCH>());
	result.push_back(MakeDropRefFunctionSet<IcebergRefOperation::DROP_TAG>());

	TableFunctionSet fast_forward(FunctionName(IcebergRefOperation::FAST_FORWARD));
	fast_forward.AddFunction(
	    MakeFunction<IcebergRefOperation::FAST_FORWARD>(FunctionSignature()
	                                                        .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                                                        .AddPositionalOnly("branch", LogicalType::VARCHAR)
	                                                        .AddPositionalOnly("to", LogicalType::VARCHAR)));
	result.push_back(std::move(fast_forward));

	TableFunctionSet set_current(FunctionName(IcebergRefOperation::SET_CURRENT));
	//! ANY so that a fractional argument is refused at bind time instead of being cast (and rounded) to an id;
	//! integers are snapshot ids and VARCHAR names a ref.
	set_current.AddFunction(
	    MakeFunction<IcebergRefOperation::SET_CURRENT>(FunctionSignature()
	                                                       .AddPositionalOnly("table_name", LogicalType::VARCHAR)
	                                                       .AddPositionalOnly("snapshot_id_or_ref", LogicalType::ANY)));
	result.push_back(std::move(set_current));
	return result;
}

} // namespace duckdb
