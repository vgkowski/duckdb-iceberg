#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void ValidateIcebergBranchName(const string &branch) {
	if (branch.empty()) {
		return;
	}
	if (branch.size() > MAX_ICEBERG_BRANCH_NAME_LENGTH) {
		throw InvalidInputException("'%s' must be at most %llu characters long", WAP_BRANCH_CONFIG_VARIABLE,
		                            static_cast<uint64_t>(MAX_ICEBERG_BRANCH_NAME_LENGTH));
	}
	for (auto c : branch) {
		auto byte = static_cast<unsigned char>(c);
		if (byte < 0x20 || byte == 0x7f) {
			throw InvalidInputException("'%s' must not contain control characters", WAP_BRANCH_CONFIG_VARIABLE);
		}
	}
	if (StringUtil::CharacterIsSpace(branch.front()) || StringUtil::CharacterIsSpace(branch.back())) {
		throw InvalidInputException("'%s' must not start or end with whitespace, got '%s'", WAP_BRANCH_CONFIG_VARIABLE,
		                            branch);
	}
	//! AT (VERSION => '<n>') reads a numeric string as a snapshot id, so such a branch could not be read back by name.
	//! Same rule as IcebergSnapshotLookup::FromVersion.
	int64_t snapshot_id;
	if (IcebergSnapshotLookup::TryParseSnapshotId(branch, snapshot_id)) {
		throw InvalidInputException("'%s' must not be a number, since a numeric VERSION names a snapshot id, got '%s'",
		                            WAP_BRANCH_CONFIG_VARIABLE, branch);
	}
	//! Ref names are case-sensitive, so 'Main' would be a separate branch that is easily mistaken for main
	if (branch != ICEBERG_MAIN_BRANCH && StringUtil::CIEquals(branch, ICEBERG_MAIN_BRANCH)) {
		throw InvalidInputException("'%s' must be exactly 'main' to target main, got '%s'", WAP_BRANCH_CONFIG_VARIABLE,
		                            branch);
	}
}

bool GetIcebergWapBranchCreateMissing(ClientContext &context) {
	Value value;
	if (!context.TryGetCurrentSetting(WAP_BRANCH_CREATE_MISSING_CONFIG_VARIABLE, value) || value.IsNull()) {
		return true;
	}
	return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
}

string GetIcebergWapBranch(ClientContext &context) {
	Value value;
	auto lookup = context.TryGetCurrentSetting(WAP_BRANCH_CONFIG_VARIABLE, value);
	if (!lookup || value.IsNull()) {
		return ICEBERG_MAIN_BRANCH;
	}
	auto branch = StringValue::Get(value);
	if (lookup.GetScope() == SettingScope::GLOBAL && !branch.empty()) {
		//! SET GLOBAL is refused; a value configured globally by other means must not redirect every session
		throw InvalidConfigurationException("'%s' can only be set for the session, not globally",
		                                    WAP_BRANCH_CONFIG_VARIABLE);
	}
	//! The setter validates too; re-check so a value set through another path can not slip through
	ValidateIcebergBranchName(branch);
	if (branch.empty()) {
		return ICEBERG_MAIN_BRANCH;
	}
	return branch;
}

IcebergOptions::IcebergOptions() : snapshot_lookup(IcebergSnapshotLookup::FromLatest()) {
}

IcebergOptions::IcebergOptions(named_argument_map_t &named_parameters) : IcebergOptions() {
	snapshot_lookup.reset();

	for (auto &kv : named_parameters) {
		auto loption = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &val = kv.second;
		if (loption == "allow_moved_paths") {
			allow_moved_paths = BooleanValue::Get(val);
		} else if (loption == "metadata_compression_codec") {
			metadata_compression_codec = StringValue::Get(val);
		} else if (loption == "version") {
			table_version = StringValue::Get(val.DefaultCastAs(LogicalType::VARCHAR));
			version_explicitly_set = true;
		} else if (loption == "version_name_format") {
			auto value = StringValue::Get(kv.second);
			auto string_substitutions = IcebergUtils::CountOccurrences(value, "%s");
			if (string_substitutions != 2) {
				throw InvalidInputException(
				    "'version_name_format' has to contain two occurrences of '%s' in it, found %d", value,
				    string_substitutions);
			}
			version_name_format = value;
		} else if (loption == "snapshot_from_id") {
			IcebergSnapshotLookup::VerifyOptionNotNull(loption, val);
			if (snapshot_lookup) {
				throw InvalidInputException(IcebergSnapshotLookup::EXCLUSIVE_OPTIONS_ERROR);
			}
			snapshot_lookup.emplace(IcebergSnapshotLookup::FromSnapshotId(val.GetValue<uint64_t>()));
		} else if (loption == "snapshot_from_ref") {
			IcebergSnapshotLookup::VerifyOptionNotNull(loption, val);
			if (snapshot_lookup) {
				throw InvalidInputException(IcebergSnapshotLookup::EXCLUSIVE_OPTIONS_ERROR);
			}
			snapshot_lookup.emplace(IcebergSnapshotLookup::FromRef(StringValue::Get(val)));
		} else if (loption == "snapshot_from_timestamp") {
			IcebergSnapshotLookup::VerifyOptionNotNull(loption, val);
			if (snapshot_lookup) {
				throw InvalidInputException(IcebergSnapshotLookup::EXCLUSIVE_OPTIONS_ERROR);
			}
			snapshot_lookup.emplace(IcebergSnapshotLookup::FromTimestamp(
			    val.DefaultCastAs(LogicalType::TIMESTAMP_MS).GetValue<timestamp_ms_t>()));
		}
	}
	if (!snapshot_lookup) {
		snapshot_lookup.emplace(IcebergSnapshotLookup::FromLatest());
	}
}

IcebergOptions &IcebergOptions::operator=(const IcebergOptions &other) {
	if (this == &other) {
		return *this;
	}
	allow_moved_paths = other.allow_moved_paths;
	metadata_compression_codec = other.metadata_compression_codec;
	infer_schema = other.infer_schema;
	table_version = other.table_version;
	version_explicitly_set = other.version_explicitly_set;
	version_name_format = other.version_name_format;
	snapshot_lookup.reset();
	if (other.snapshot_lookup) {
		snapshot_lookup.emplace(*other.snapshot_lookup);
	}
	return *this;
}

} // namespace duckdb
