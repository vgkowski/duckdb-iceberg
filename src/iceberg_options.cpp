#include "iceberg_options.hpp"
#include "common/iceberg_utils.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

//! The zero of every run of ten decimal digits (Unicode category Nd) in the Basic Multilingual Plane other than ASCII.
//! Java's Character.digit works on UTF-16 code units, so only these are read as digits by Long.parseLong.
static constexpr uint32_t NON_ASCII_DIGIT_ZEROS[] = {
    0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6, 0x0D66,
    0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80, 0x1A90,
    0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10};

static bool ContainsNonAsciiDecimalDigit(const string &text) {
	for (idx_t i = 0; i < text.size();) {
		auto lead = static_cast<unsigned char>(text[i]);
		uint32_t codepoint;
		idx_t length;
		if (lead < 0x80) {
			i++;
			continue;
		} else if ((lead & 0xE0) == 0xC0) {
			codepoint = lead & 0x1F;
			length = 2;
		} else if ((lead & 0xF0) == 0xE0) {
			codepoint = lead & 0x0F;
			length = 3;
		} else {
			//! Four-byte sequences are outside the BMP (surrogate pairs in Java); invalid lead bytes are skipped
			i += (lead & 0xF8) == 0xF0 ? 4 : 1;
			continue;
		}
		if (i + length > text.size()) {
			return false;
		}
		for (idx_t k = 1; k < length; k++) {
			codepoint = (codepoint << 6) | (static_cast<unsigned char>(text[i + k]) & 0x3F);
		}
		for (auto zero : NON_ASCII_DIGIT_ZEROS) {
			if (codepoint >= zero && codepoint < zero + 10) {
				return true;
			}
		}
		i += length;
	}
	return false;
}

void ValidateIcebergRefName(const string &name, const string &subject) {
	if (name.empty()) {
		throw InvalidInputException("%s must not be empty", subject);
	}
	if (name.size() > MAX_ICEBERG_REF_NAME_LENGTH) {
		throw InvalidInputException("%s must be at most %llu bytes long", subject,
		                            static_cast<uint64_t>(MAX_ICEBERG_REF_NAME_LENGTH));
	}
	for (auto c : name) {
		auto byte = static_cast<unsigned char>(c);
		if (byte < 0x20 || byte == 0x7f) {
			throw InvalidInputException("%s must not contain control characters", subject);
		}
	}
	if (StringUtil::CharacterIsSpace(name.front()) || StringUtil::CharacterIsSpace(name.back())) {
		throw InvalidInputException("%s must not start or end with whitespace, got '%s'", subject, name);
	}
	//! Java's Long.parseLong (Spark's VERSION AS OF) also reads non-ASCII decimal digits such as '١٢٣', so such a name
	//! could be a ref here but a snapshot id elsewhere
	if (ContainsNonAsciiDecimalDigit(name)) {
		throw InvalidInputException("%s must not contain non-ASCII digits, got '%s'", subject, name);
	}
	//! AT (VERSION => '<n>') reads an optional sign then ASCII digits as a snapshot id (IcebergSnapshotLookup::
	//! TryParseSnapshotId, like Long.parseLong), so such a ref could not be read back by name. Every such name is
	//! refused, including ones out of int64 range, which a reader may still take for a snapshot id.
	idx_t first_digit = (name[0] == '+' || name[0] == '-') ? 1 : 0;
	bool all_digits = first_digit < name.size();
	for (idx_t i = first_digit; i < name.size() && all_digits; i++) {
		all_digits = StringUtil::CharacterIsDigit(name[i]);
	}
	if (all_digits) {
		throw InvalidInputException("%s must not be a number, since a numeric VERSION names a snapshot id, got '%s'",
		                            subject, name);
	}
	//! Ref names are case-sensitive, so 'Main' would be a separate branch that is easily mistaken for main
	if (name != ICEBERG_MAIN_BRANCH && StringUtil::CIEquals(name, ICEBERG_MAIN_BRANCH)) {
		throw InvalidInputException("%s must be exactly 'main' to name main, got '%s'", subject, name);
	}
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
