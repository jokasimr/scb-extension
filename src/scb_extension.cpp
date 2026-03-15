#define DUCKDB_EXTENSION_MAIN

#include "scb_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "httplib.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <cstdlib>

using namespace duckdb_yyjson; // NOLINT
namespace scb_httplib = CPPHTTPLIB_NAMESPACE;

namespace duckdb {
namespace {

static constexpr const char *DEFAULT_SCB_BASE_URL = "https://statistikdatabasen.scb.se/api/v2";
static constexpr idx_t SCB_TABLE_PAGE_SIZE = 1000;

struct ScbConfigRow {
	string api_version;
	string app_version;
	string default_language;
	vector<string> languages;
	int64_t max_data_cells = 0;
	int64_t max_calls_per_time_window = 0;
	int64_t time_window = 0;
	string default_data_format;
	vector<string> data_formats;
};

struct ScbTableRow {
	string language;
	string table_id;
	string label;
	string description;
	string updated;
	string first_period;
	string last_period;
	bool discontinued = false;
	string source;
	string subject_code;
	string time_unit;
	string category;
	string path_labels;
	string path_ids;
};

struct ScbCodelistRef {
	string id;
	string label;
	string type;
};

struct ScbValueMetadata {
	string value_code;
	string label;
	int64_t sort_index = 0;
	string parent_code;
	int64_t child_count = 0;
};

struct ScbVariableMetadata {
	string variable_code;
	string label;
	string role;
	vector<ScbCodelistRef> codelists;
	vector<ScbValueMetadata> values;
};

struct ScbVariableRow {
	string table_id;
	string language;
	string variable_code;
	string role;
	int64_t variant = 0;
	string variant_name;
	string variant_kind;
	int64_t member_count = 0;
};

struct ScbValueRow {
	string table_id;
	string language;
	string variable_code;
	string value_code;
	string label;
	int64_t sort_index = 0;
	string parent_code;
	int64_t child_count = 0;
};

struct ScbSelectionItem {
	string variable_code;
	string codelist;
	vector<string> value_codes;
};

struct ScbCodelistValue {
	string value_code;
	string label;
};

struct ScbLabeledValue {
	string value_code;
	string label;
};

struct ScbDimensionVariant {
	string codelist_id;
	string variant_name;
	string variant_kind;
	vector<ScbLabeledValue> values;
};

struct ScbRequestedVariant {
	bool by_index = true;
	int64_t variant_index = 0;
	string variant_name;
};

struct ScbOutputDimension {
	string variable_code;
	string label;
	unordered_map<string, string> labels_by_code;
};

struct ScbMetricColumn {
	string value_code;
	string label;
};

class JSONDoc {
public:
	explicit JSONDoc(const string &body) {
		doc = yyjson_read(body.data(), body.size(), YYJSON_READ_NOFLAG);
		if (!doc) {
			throw InvalidInputException("SCB returned invalid JSON");
		}
	}

	~JSONDoc() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}

	yyjson_val *Root() const {
		return yyjson_doc_get_root(doc);
	}

private:
	yyjson_doc *doc;
};

static string GetScbBaseURL() {
	auto *override_base_url = std::getenv("SCB_API_BASE_URL");
	if (override_base_url && override_base_url[0] != '\0') {
		return override_base_url;
	}
	return DEFAULT_SCB_BASE_URL;
}

static string JSONEscape(const string &input) {
	string result;
	result.reserve(input.size() + 8);
	for (auto c : input) {
		switch (c) {
		case '\\':
			result += "\\\\";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				result += StringUtil::Format("\\u%04x", static_cast<unsigned char>(c));
			} else {
				result += c;
			}
			break;
		}
	}
	return result;
}

static optional_ptr<yyjson_val> RequireObject(yyjson_val *value, const string &what) {
	if (!value || !yyjson_is_obj(value)) {
		throw InvalidInputException("Expected %s to be a JSON object", what);
	}
	return value;
}

static optional_ptr<yyjson_val> RequireArray(yyjson_val *value, const string &what) {
	if (!value || !yyjson_is_arr(value)) {
		throw InvalidInputException("Expected %s to be a JSON array", what);
	}
	return value;
}

static string GetRequiredString(yyjson_val *object, const char *key) {
	auto value = yyjson_obj_get(object, key);
	if (!value || !yyjson_is_str(value)) {
		throw InvalidInputException("Expected SCB JSON field \"%s\" to be a string", key);
	}
	return yyjson_get_str(value);
}

static string GetOptionalString(yyjson_val *object, const char *key) {
	auto value = yyjson_obj_get(object, key);
	if (!value || yyjson_is_null(value)) {
		return string();
	}
	if (!yyjson_is_str(value)) {
		throw InvalidInputException("Expected SCB JSON field \"%s\" to be a string", key);
	}
	return yyjson_get_str(value);
}

static bool GetOptionalBool(yyjson_val *object, const char *key, bool default_value = false) {
	auto value = yyjson_obj_get(object, key);
	if (!value || yyjson_is_null(value)) {
		return default_value;
	}
	if (!yyjson_is_bool(value)) {
		throw InvalidInputException("Expected SCB JSON field \"%s\" to be a boolean", key);
	}
	return yyjson_get_bool(value);
}

static int64_t GetOptionalInteger(yyjson_val *object, const char *key, int64_t default_value = 0) {
	auto value = yyjson_obj_get(object, key);
	if (!value || yyjson_is_null(value)) {
		return default_value;
	}
	if (!yyjson_is_int(value) && !yyjson_is_uint(value)) {
		throw InvalidInputException("Expected SCB JSON field \"%s\" to be an integer", key);
	}
	return yyjson_get_sint(value);
}

static vector<string> ParseStringArray(yyjson_val *value, const string &what) {
	vector<string> result;
	if (!value || yyjson_is_null(value)) {
		return result;
	}
	if (yyjson_is_str(value)) {
		result.emplace_back(yyjson_get_str(value));
		return result;
	}
	RequireArray(value, what);
	yyjson_val *entry;
	size_t idx, max;
	yyjson_arr_foreach(value, idx, max, entry) {
		if (yyjson_is_str(entry)) {
			result.emplace_back(yyjson_get_str(entry));
			continue;
		}
		if (yyjson_is_obj(entry)) {
			auto id = yyjson_obj_get(entry, "id");
			if (id && yyjson_is_str(id)) {
				result.emplace_back(yyjson_get_str(id));
				continue;
			}
		}
		throw InvalidInputException("Expected %s entries to be strings or {id: ...} objects", what);
	}
	return result;
}

static vector<string> GetStringArray(yyjson_val *object, const char *key) {
	return ParseStringArray(yyjson_obj_get(object, key), key);
}

static vector<Value> ToValueList(const vector<string> &values) {
	vector<Value> result;
	result.reserve(values.size());
	for (const auto &value : values) {
		result.emplace_back(value);
	}
	return result;
}

static idx_t EstimateStringMemory(const string &value) {
	return value.size();
}

static idx_t EstimateStringVectorMemory(const vector<string> &values) {
	idx_t result = 0;
	for (const auto &value : values) {
		result += value.size();
	}
	return result;
}

static string BuildCacheKey(const string &kind, const string &identity) {
	return "scb:" + kind + ":" + identity;
}

static ScbLabeledValue MakeLabeledValue(const string &value_code, const string &label) {
	return {value_code, label.empty() ? value_code : label};
}

static scb_httplib::Headers ToHTTPLibHeaders(const HTTPHeaders &headers) {
	scb_httplib::Headers result;
	for (const auto &entry : headers) {
		result.insert({entry.first, entry.second});
	}
	return result;
}

static unique_ptr<scb_httplib::Client> CreateHTTPClient(const string &url, HTTPParams &params) {
	string path;
	string proto_host_port;
	HTTPUtil::DecomposeURL(url, path, proto_host_port);
	auto client = make_uniq<scb_httplib::Client>(proto_host_port);
	client->set_follow_location(params.follow_location);
	client->set_keep_alive(params.keep_alive);
	client->set_connection_timeout(static_cast<time_t>(params.timeout), static_cast<time_t>(params.timeout_usec));
	client->set_read_timeout(static_cast<time_t>(params.timeout), static_cast<time_t>(params.timeout_usec));
	client->set_write_timeout(static_cast<time_t>(params.timeout), static_cast<time_t>(params.timeout_usec));
	client->set_decompress(false);
	if (!params.http_proxy.empty()) {
		client->set_proxy(params.http_proxy, static_cast<int>(params.http_proxy_port));
		if (!params.http_proxy_username.empty()) {
			client->set_proxy_basic_auth(params.http_proxy_username, params.http_proxy_password);
		}
	}
	return client;
}

static unique_ptr<HTTPParams> InitializeHTTPParams(ClientContext &context, const string &url) {
	auto &http_util = HTTPUtil::Get(*context.db);
	auto params = http_util.InitializeParameters(context, url);
	params->retry_wait_ms = MaxValue<uint64_t>(params->retry_wait_ms, 500);
	params->retry_backoff = 2.0;
	params->retries = MaxValue<uint64_t>(params->retries, 6);
	return params;
}

static void ThrowHTTPFailure(const string &method, const string &url, const HTTPResponse &response) {
	if (response.HasRequestError()) {
		throw IOException("SCB %s failed for \"%s\": %s", method, url, response.GetRequestError());
	}
	throw IOException("SCB %s failed for \"%s\": HTTP %d %s", method, url, static_cast<int>(response.status),
	                  response.reason);
}

static unique_ptr<HTTPResponse> ToHTTPResponse(const scb_httplib::Result &result) {
	if (result.error() != scb_httplib::Error::Success) {
		auto failure = make_uniq<HTTPResponse>(HTTPStatusCode::INVALID);
		failure->request_error = to_string(result.error());
		return failure;
	}
	auto success = make_uniq<HTTPResponse>(HTTPUtil::ToStatusCode(result->status));
	success->body = result->body;
	success->reason = result->reason;
	for (const auto &entry : result->headers) {
		success->headers.Insert(entry.first, entry.second);
	}
	return success;
}

template <class REQUEST, class INVOKE>
static string RunHTTPRequest(const string &method, const string &url, unique_ptr<HTTPParams> params, REQUEST &request,
                             INVOKE invoke) {
	auto client = CreateHTTPClient(url, *params);
	auto response = HTTPUtil::RunRequestWithRetry(
	    [&]() {
		    return ToHTTPResponse(invoke(*client, request));
	    },
	    request, [&]() { client = CreateHTTPClient(url, *params); });
	if (!response->Success()) {
		ThrowHTTPFailure(method, url, *response);
	}
	return response->body;
}

static string HTTPGet(ClientContext &context, const string &url, const string &accept = "application/json") {
	auto params = InitializeHTTPParams(context, url);
	HTTPHeaders request_headers(*context.db);
	request_headers.Insert("Accept", accept);
	GetRequestInfo request(url, request_headers, *params, nullptr, nullptr);
	request.try_request = true;
	return RunHTTPRequest("GET", url, std::move(params), request, [](scb_httplib::Client &client, GetRequestInfo &request) {
		return client.Get(request.path, ToHTTPLibHeaders(request.headers));
	});
}

static string HTTPPostJSON(ClientContext &context, const string &url, const string &body,
                           const string &accept = "application/json") {
	auto params = InitializeHTTPParams(context, url);
	HTTPHeaders request_headers(*context.db);
	request_headers.Insert("Accept", accept);
	request_headers.Insert("Content-Type", "application/json");
	PostRequestInfo request(url, request_headers, *params, const_data_ptr_cast(body.data()), body.size());
	request.try_request = true;
	return RunHTTPRequest("POST", url, std::move(params), request,
	                      [&](scb_httplib::Client &client, PostRequestInfo &request) {
		                      return client.Post(request.path, ToHTTPLibHeaders(request.headers), body,
		                                         "application/json");
	                      });
}

template <class ENTRY, class LOADER>
static shared_ptr<ENTRY> GetOrLoadCacheEntry(ClientContext &context, const string &cache_key, LOADER loader) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto existing = cache.Get<ENTRY>(cache_key);
	if (existing) {
		return existing;
	}
	auto loaded = loader();
	cache.Put(cache_key, loaded);
	return loaded;
}

class ScbConfigCacheEntry : public ObjectCacheEntry {
public:
	explicit ScbConfigCacheEntry(ScbConfigRow row_p) : row(std::move(row_p)) {
	}

	static string ObjectType() {
		return "scb_config";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return EstimateStringMemory(row.api_version) + EstimateStringMemory(row.app_version) +
		       EstimateStringMemory(row.default_language) + EstimateStringVectorMemory(row.languages) +
		       EstimateStringMemory(row.default_data_format) + EstimateStringVectorMemory(row.data_formats);
	}

	ScbConfigRow row;
};

class ScbTablesCacheEntry : public ObjectCacheEntry {
public:
	explicit ScbTablesCacheEntry(vector<ScbTableRow> rows_p) : rows(std::move(rows_p)) {
	}

	static string ObjectType() {
		return "scb_tables";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		idx_t total = 0;
		for (const auto &row : rows) {
			total += EstimateStringMemory(row.language) + EstimateStringMemory(row.table_id) +
			         EstimateStringMemory(row.label) + EstimateStringMemory(row.description) +
			         EstimateStringMemory(row.updated) + EstimateStringMemory(row.first_period) +
			         EstimateStringMemory(row.last_period) + EstimateStringMemory(row.source) +
			         EstimateStringMemory(row.subject_code) + EstimateStringMemory(row.time_unit) +
			         EstimateStringMemory(row.category) + EstimateStringMemory(row.path_labels) +
			         EstimateStringMemory(row.path_ids);
		}
		return total;
	}

	vector<ScbTableRow> rows;
};

class ScbMetadataCacheEntry : public ObjectCacheEntry {
public:
	ScbMetadataCacheEntry(string table_id_p, string language_p, vector<ScbVariableMetadata> variables_p)
	    : table_id(std::move(table_id_p)), language(std::move(language_p)), variables(std::move(variables_p)) {
	}

	static string ObjectType() {
		return "scb_metadata";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		idx_t total = EstimateStringMemory(table_id) + EstimateStringMemory(language);
		for (const auto &variable : variables) {
			total += EstimateStringMemory(variable.variable_code) + EstimateStringMemory(variable.label) +
			         EstimateStringMemory(variable.role);
			for (const auto &codelist : variable.codelists) {
				total += EstimateStringMemory(codelist.id) + EstimateStringMemory(codelist.label) +
				         EstimateStringMemory(codelist.type);
			}
			for (const auto &value : variable.values) {
				total += EstimateStringMemory(value.value_code) + EstimateStringMemory(value.label) +
				         EstimateStringMemory(value.parent_code);
			}
		}
		return total;
	}

	string table_id;
	string language;
	vector<ScbVariableMetadata> variables;
};

class ScbCodelistCacheEntry : public ObjectCacheEntry {
public:
	ScbCodelistCacheEntry(string label_p, string type_p, vector<ScbCodelistValue> values_p)
	    : label(std::move(label_p)), type(std::move(type_p)), values(std::move(values_p)) {
	}

	static string ObjectType() {
		return "scb_codelist";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		idx_t total = EstimateStringMemory(label) + EstimateStringMemory(type);
		for (const auto &value : values) {
			total += EstimateStringMemory(value.value_code) + EstimateStringMemory(value.label);
		}
		return total;
	}

	string label;
	string type;
	vector<ScbCodelistValue> values;
};

class ScbDerivedVariantsCacheEntry : public ObjectCacheEntry {
public:
	explicit ScbDerivedVariantsCacheEntry(unordered_map<string, vector<ScbDimensionVariant>> variants_p)
	    : variants_by_variable(std::move(variants_p)) {
	}

	static string ObjectType() {
		return "scb_dimension_variants";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		idx_t total = 0;
		for (const auto &entry : variants_by_variable) {
			total += EstimateStringMemory(entry.first);
			for (const auto &variant : entry.second) {
				total += EstimateStringMemory(variant.codelist_id) + EstimateStringMemory(variant.variant_name) +
				         EstimateStringMemory(variant.variant_kind);
				for (const auto &value : variant.values) {
					total += EstimateStringMemory(value.value_code) + EstimateStringMemory(value.label);
				}
			}
		}
		return total;
	}

	unordered_map<string, vector<ScbDimensionVariant>> variants_by_variable;
};

class ScbMaterializedScanChunkCacheEntry : public ObjectCacheEntry {
public:
	explicit ScbMaterializedScanChunkCacheEntry(unique_ptr<ColumnDataCollection> rows_p)
	    : rows(std::move(rows_p)) {
	}

	static string ObjectType() {
		return "scb_scan_chunk";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return rows ? rows->SizeInBytes() : 0;
	}

	unique_ptr<ColumnDataCollection> rows;
};

static shared_ptr<ScbConfigCacheEntry> LoadConfigEntry(ClientContext &context) {
	return GetOrLoadCacheEntry<ScbConfigCacheEntry>(context, BuildCacheKey("config", "default"), [&]() {
		auto body = HTTPGet(context, GetScbBaseURL() + "/config");
		JSONDoc doc(body);
		auto root = RequireObject(doc.Root(), "config response");

		ScbConfigRow row;
		row.api_version = GetRequiredString(root.get(), "apiVersion");
		row.app_version = GetRequiredString(root.get(), "appVersion");
		row.default_language = GetRequiredString(root.get(), "defaultLanguage");
		row.languages = GetStringArray(root.get(), "languages");
		row.max_data_cells = GetOptionalInteger(root.get(), "maxDataCells");
		row.max_calls_per_time_window = GetOptionalInteger(root.get(), "maxCallsPerTimeWindow");
		row.time_window = GetOptionalInteger(root.get(), "timeWindow");
		row.default_data_format = GetRequiredString(root.get(), "defaultDataFormat");
		row.data_formats = GetStringArray(root.get(), "dataFormats");
		return make_shared_ptr<ScbConfigCacheEntry>(std::move(row));
	});
}

static string JoinPathDimension(yyjson_val *path_entry, const char *field) {
	if (!path_entry || !yyjson_is_arr(path_entry)) {
		return string();
	}
	vector<string> parts;
	yyjson_val *segment;
	size_t idx, max;
	yyjson_arr_foreach(path_entry, idx, max, segment) {
		if (!yyjson_is_obj(segment)) {
			continue;
		}
		auto value = GetOptionalString(segment, field);
		if (!value.empty()) {
			parts.push_back(value);
		}
	}
	return StringUtil::Join(parts, " / ");
}

static shared_ptr<ScbTablesCacheEntry> LoadTablesEntry(ClientContext &context, const string &lang,
                                                       const string &query, bool include_discontinued,
                                                       optional_idx past_days) {
	auto identity = StringUtil::Format("lang=%s|query=%s|include_discontinued=%d|past_days=%s", lang, query,
	                                   include_discontinued ? 1 : 0,
	                                   past_days.IsValid() ? to_string(past_days.GetIndex()) : "null");
	return GetOrLoadCacheEntry<ScbTablesCacheEntry>(context, BuildCacheKey("tables", identity), [&]() {
		vector<ScbTableRow> rows;
		idx_t page_number = 1;
		idx_t total_pages = 1;

		while (page_number <= total_pages) {
			string url = StringUtil::Format("%s/tables?lang=%s&pageNumber=%llu&pageSize=%llu", GetScbBaseURL(),
			                                StringUtil::URLEncode(lang), static_cast<unsigned long long>(page_number),
			                                static_cast<unsigned long long>(SCB_TABLE_PAGE_SIZE));
			if (!query.empty()) {
				url += "&query=" + StringUtil::URLEncode(query);
			}
			if (include_discontinued) {
				url += "&includeDiscontinued=true";
			}
			if (past_days.IsValid()) {
				url += "&pastDays=" + to_string(past_days.GetIndex());
			}

			auto body = HTTPGet(context, url);
			JSONDoc doc(body);
			auto root = RequireObject(doc.Root(), "tables response");
			auto tables = RequireArray(yyjson_obj_get(root.get(), "tables"), "tables");
			auto page = RequireObject(yyjson_obj_get(root.get(), "page"), "page");
			total_pages = NumericCast<idx_t>(GetOptionalInteger(page.get(), "totalPages", 1));

			yyjson_val *table;
			size_t idx, max;
			yyjson_arr_foreach(tables.get(), idx, max, table) {
				if (!yyjson_is_obj(table)) {
					continue;
				}
				ScbTableRow row;
				row.language = lang;
				row.table_id = GetRequiredString(table, "id");
				row.label = GetOptionalString(table, "label");
				row.description = GetOptionalString(table, "description");
				row.updated = GetOptionalString(table, "updated");
				row.first_period = GetOptionalString(table, "firstPeriod");
				row.last_period = GetOptionalString(table, "lastPeriod");
				row.discontinued = GetOptionalBool(table, "discontinued");
				row.source = GetOptionalString(table, "source");
				row.subject_code = GetOptionalString(table, "subjectCode");
				row.time_unit = GetOptionalString(table, "timeUnit");
				row.category = GetOptionalString(table, "category");

				auto paths = yyjson_obj_get(table, "paths");
				if (paths && yyjson_is_arr(paths) && yyjson_arr_size(paths) > 0) {
					auto first_path = yyjson_arr_get(paths, 0);
					row.path_labels = JoinPathDimension(first_path, "label");
					row.path_ids = JoinPathDimension(first_path, "id");
				}
				rows.push_back(std::move(row));
			}
			page_number++;
		}
		return make_shared_ptr<ScbTablesCacheEntry>(std::move(rows));
	});
}

static unordered_map<string, string> ParseRoleMap(yyjson_val *root) {
	unordered_map<string, string> result;
	auto role = yyjson_obj_get(root, "role");
	if (!role || !yyjson_is_obj(role)) {
		return result;
	}
	yyjson_val *key, *value;
	size_t idx, max;
	yyjson_obj_foreach(role, idx, max, key, value) {
		if (!yyjson_is_arr(value)) {
			continue;
		}
		const auto role_name = string(yyjson_get_str(key));
		yyjson_val *code;
		size_t arr_idx, arr_max;
		yyjson_arr_foreach(value, arr_idx, arr_max, code) {
			if (!yyjson_is_str(code)) {
				continue;
			}
			auto variable_code = string(yyjson_get_str(code));
			auto entry = result.find(variable_code);
			if (entry == result.end()) {
				result.emplace(std::move(variable_code), role_name);
			} else {
				entry->second += "," + role_name;
			}
		}
	}
	return result;
}

static yyjson_val *GetCodeListsNode(yyjson_val *extension) {
	if (!extension || !yyjson_is_obj(extension)) {
		return nullptr;
	}
	auto code_lists = yyjson_obj_get(extension, "codeLists");
	if (code_lists) {
		return code_lists;
	}
	return yyjson_obj_get(extension, "codelists");
}

static string SelectionItemsToJSON(const vector<ScbSelectionItem> &items);

static shared_ptr<ScbCodelistCacheEntry> LoadCodelistEntry(ClientContext &context, const string &codelist_id,
                                                           const string &lang) {
	auto identity = codelist_id + "|" + lang;
	return GetOrLoadCacheEntry<ScbCodelistCacheEntry>(context, BuildCacheKey("codelist", identity), [&]() {
		auto url = StringUtil::Format("%s/codelists/%s?lang=%s", GetScbBaseURL(), StringUtil::URLEncode(codelist_id),
		                              StringUtil::URLEncode(lang));
		auto body = HTTPGet(context, url);
		JSONDoc doc(body);
		auto root = RequireObject(doc.Root(), "codelist response");

		vector<ScbCodelistValue> values;
		auto values_node = yyjson_obj_get(root.get(), "values");
		if (values_node && yyjson_is_arr(values_node)) {
			yyjson_val *value_entry;
			size_t idx, max;
			yyjson_arr_foreach(values_node, idx, max, value_entry) {
				if (!value_entry || !yyjson_is_obj(value_entry)) {
					continue;
				}
				ScbCodelistValue row;
				row.value_code = GetRequiredString(value_entry, "code");
				row.label = GetOptionalString(value_entry, "label");
				values.push_back(std::move(row));
			}
		}
		return make_shared_ptr<ScbCodelistCacheEntry>(GetOptionalString(root.get(), "label"),
		                                             GetOptionalString(root.get(), "type"), std::move(values));
		});
}

static shared_ptr<ScbCodelistCacheEntry> ProbeCodelistEntryFromData(ClientContext &context,
                                                                    const ScbMetadataCacheEntry &metadata,
                                                                    const string &variable_code, const string &codelist_id,
                                                                    const string &codelist_label, const string &codelist_type) {
	auto identity =
	    StringUtil::Format("%s|%s|%s|%s", metadata.table_id, variable_code, codelist_id, metadata.language);
	return GetOrLoadCacheEntry<ScbCodelistCacheEntry>(context, BuildCacheKey("codelist_probe", identity), [&]() {
		    vector<ScbSelectionItem> selection_items;
		    for (const auto &variable : metadata.variables) {
			    ScbSelectionItem item;
			    item.variable_code = variable.variable_code;
			    if (variable.variable_code == variable_code) {
				    item.codelist = codelist_id;
				    item.value_codes.push_back("*");
				    selection_items.push_back(std::move(item));
				    continue;
			    }

			    if (variable.values.empty()) {
				    continue;
			    }
			    item.value_codes.push_back(variable.values[0].value_code);
			    selection_items.push_back(std::move(item));
		    }

		    auto data_url = StringUtil::Format("%s/tables/%s/data?lang=%s&outputFormat=json-stat2", GetScbBaseURL(),
		                                       StringUtil::URLEncode(metadata.table_id),
		                                       StringUtil::URLEncode(metadata.language));
		    auto body = HTTPPostJSON(context, data_url, SelectionItemsToJSON(selection_items), "application/json");
		    JSONDoc doc(body);
		    auto root = RequireObject(doc.Root(), "data probe response");
		    auto dimension = RequireObject(yyjson_obj_get(root.get(), "dimension"), "data probe dimension");
		    auto variable_node = RequireObject(yyjson_obj_get(dimension.get(), variable_code.c_str()),
		                                       "data probe variable");
		    auto category = RequireObject(yyjson_obj_get(variable_node.get(), "category"), "data probe category");
		    auto index_object = RequireObject(yyjson_obj_get(category.get(), "index"), "data probe category index");
		    auto label_object = yyjson_obj_get(category.get(), "label");

		    vector<ScbCodelistValue> values;
		    yyjson_val *key, *value;
		    size_t idx, max;
		    yyjson_obj_foreach(index_object.get(), idx, max, key, value) {
			    if (!yyjson_is_str(key)) {
				    continue;
			    }
			    ScbCodelistValue row;
			    row.value_code = yyjson_get_str(key);
			    row.label = row.value_code;
			    if (label_object && yyjson_is_obj(label_object)) {
				    auto label = yyjson_obj_get(label_object, row.value_code.c_str());
				    if (label && yyjson_is_str(label)) {
					    row.label = yyjson_get_str(label);
				    }
			    }
			    values.push_back(std::move(row));
		    }
		    return make_shared_ptr<ScbCodelistCacheEntry>(codelist_label, codelist_type, std::move(values));
	    });
}

static shared_ptr<ScbMetadataCacheEntry> LoadMetadataEntry(ClientContext &context, const string &table_id,
                                                           const string &lang) {
	auto identity = table_id + "|" + lang;
	return GetOrLoadCacheEntry<ScbMetadataCacheEntry>(context, BuildCacheKey("metadata", identity), [&]() {
		auto url = StringUtil::Format("%s/tables/%s/metadata?lang=%s", GetScbBaseURL(), StringUtil::URLEncode(table_id),
		                              StringUtil::URLEncode(lang));
		auto body = HTTPGet(context, url);
		JSONDoc doc(body);
		auto root = RequireObject(doc.Root(), "metadata response");
		auto dimension = RequireObject(yyjson_obj_get(root.get(), "dimension"), "dimension");
		auto ids = RequireArray(yyjson_obj_get(root.get(), "id"), "id");
		auto role_map = ParseRoleMap(root.get());

		vector<ScbVariableMetadata> variables;

		yyjson_val *id_value;
		size_t idx, max;
		yyjson_arr_foreach(ids.get(), idx, max, id_value) {
			if (!yyjson_is_str(id_value)) {
				continue;
			}
			const auto variable_code = string(yyjson_get_str(id_value));
			auto variable = yyjson_obj_get(dimension.get(), variable_code.c_str());
			if (!variable || !yyjson_is_obj(variable)) {
				continue;
			}

			ScbVariableMetadata variable_row;
			variable_row.variable_code = variable_code;
			variable_row.label = GetOptionalString(variable, "label");
			auto role_entry = role_map.find(variable_code);
			if (role_entry != role_map.end()) {
				variable_row.role = role_entry->second;
			}

			auto extension = yyjson_obj_get(variable, "extension");
			if (extension && yyjson_is_obj(extension)) {
				auto codelists = GetCodeListsNode(extension);
				if (codelists && yyjson_is_arr(codelists)) {
					yyjson_val *codelist_entry;
					size_t codelist_idx, codelist_max;
					yyjson_arr_foreach(codelists, codelist_idx, codelist_max, codelist_entry) {
						if (!codelist_entry || !yyjson_is_obj(codelist_entry)) {
							continue;
						}
						ScbCodelistRef codelist_ref;
						codelist_ref.id = GetOptionalString(codelist_entry, "id");
						codelist_ref.label = GetOptionalString(codelist_entry, "label");
						codelist_ref.type = GetOptionalString(codelist_entry, "type");
						variable_row.codelists.push_back(std::move(codelist_ref));
					}
				}
			}

			auto category = yyjson_obj_get(variable, "category");
			if (!category || !yyjson_is_obj(category)) {
				variables.push_back(std::move(variable_row));
				continue;
			}

			auto index_object = yyjson_obj_get(category, "index");
			auto label_object = yyjson_obj_get(category, "label");
			auto child_object = yyjson_obj_get(category, "child");
			vector<pair<int64_t, string>> ordered_codes;
			unordered_map<string, int64_t> child_count;
			unordered_map<string, string> parent_of;

			if (child_object && yyjson_is_obj(child_object)) {
				yyjson_val *child_key, *child_values;
				size_t child_idx, child_max;
				yyjson_obj_foreach(child_object, child_idx, child_max, child_key, child_values) {
					if (!yyjson_is_arr(child_values)) {
						continue;
					}
					const auto parent_code = string(yyjson_get_str(child_key));
					child_count[parent_code] = NumericCast<int64_t>(yyjson_arr_size(child_values));
					yyjson_val *child_value;
					size_t arr_idx, arr_max;
					yyjson_arr_foreach(child_values, arr_idx, arr_max, child_value) {
						if (yyjson_is_str(child_value)) {
							parent_of[string(yyjson_get_str(child_value))] = parent_code;
						}
					}
				}
			}

			if (index_object && yyjson_is_obj(index_object)) {
				yyjson_val *code_key, *position;
				size_t index_idx, index_max;
				yyjson_obj_foreach(index_object, index_idx, index_max, code_key, position) {
					if (!yyjson_is_int(position) && !yyjson_is_uint(position)) {
						continue;
					}
					ordered_codes.emplace_back(yyjson_get_sint(position), yyjson_get_str(code_key));
				}
			}
			std::sort(ordered_codes.begin(), ordered_codes.end(),
			          [](const pair<int64_t, string> &left, const pair<int64_t, string> &right) {
				          return left.first < right.first;
			          });
			for (const auto &entry : ordered_codes) {
				ScbValueMetadata value_row;
				value_row.value_code = entry.second;
				value_row.sort_index = entry.first;
				if (label_object && yyjson_is_obj(label_object)) {
					auto label = yyjson_obj_get(label_object, value_row.value_code.c_str());
					if (label && yyjson_is_str(label)) {
						value_row.label = yyjson_get_str(label);
					}
				}
				auto parent_entry = parent_of.find(value_row.value_code);
				if (parent_entry != parent_of.end()) {
					value_row.parent_code = parent_entry->second;
				}
				auto child_entry = child_count.find(value_row.value_code);
				if (child_entry != child_count.end()) {
					value_row.child_count = child_entry->second;
				}
				variable_row.values.push_back(std::move(value_row));
			}
			variables.push_back(std::move(variable_row));
		}

		return make_shared_ptr<ScbMetadataCacheEntry>(table_id, lang, std::move(variables));
	});
}

static string SelectionItemsToJSON(const vector<ScbSelectionItem> &items) {
	if (items.empty()) {
		return string();
	}
	string result = "{\"selection\":[";
	for (idx_t item_idx = 0; item_idx < items.size(); item_idx++) {
		const auto &item = items[item_idx];
		if (item_idx > 0) {
			result += ",";
		}
		result += "{\"variableCode\":\"" + JSONEscape(item.variable_code) + "\"";
		if (!item.codelist.empty()) {
			result += ",\"codelist\":\"" + JSONEscape(item.codelist) + "\"";
		}
		result += ",\"valueCodes\":[";
		for (idx_t value_idx = 0; value_idx < item.value_codes.size(); value_idx++) {
			if (value_idx > 0) {
				result += ",";
			}
			result += "\"" + JSONEscape(item.value_codes[value_idx]) + "\"";
		}
		result += "]}";
	}
	result += "]}";
	return result;
}

static bool IsMetricRole(const string &role) {
	return StringUtil::Contains(StringUtil::Lower(role), "metric");
}

static ScbRequestedVariant ParseRequestedVariantValue(const Value &value) {
	ScbRequestedVariant result;
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		result.by_index = false;
		result.variant_name = value.ToString();
		return result;
	}
	if (value.type().IsIntegral()) {
		result.by_index = true;
		result.variant_index = value.GetValue<int64_t>();
		return result;
	}
	throw InvalidInputException("SCB variant values must be integers or strings");
}

static case_insensitive_map_t<ScbRequestedVariant> ParseVariantsJSON(const string &variants_json) {
	case_insensitive_map_t<ScbRequestedVariant> result;
	auto trimmed = variants_json;
	StringUtil::Trim(trimmed);
	if (trimmed.empty()) {
		return result;
	}
	JSONDoc doc(trimmed);
	auto root = doc.Root();
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("SCB variants JSON must be an object");
	}
	yyjson_val *key, *value;
	size_t idx, max;
	yyjson_obj_foreach(root, idx, max, key, value) {
		ScbRequestedVariant requested_variant;
		if (yyjson_is_int(value) || yyjson_is_uint(value)) {
			requested_variant.by_index = true;
			requested_variant.variant_index = yyjson_get_sint(value);
		} else if (yyjson_is_str(value)) {
			requested_variant.by_index = false;
			requested_variant.variant_name = yyjson_get_str(value);
		} else {
			throw InvalidInputException("SCB variants JSON values must be integers or strings");
		}
		result[yyjson_get_str(key)] = std::move(requested_variant);
	}
	return result;
}

static case_insensitive_map_t<ScbRequestedVariant> ParseVariantsValue(const Value &value) {
	case_insensitive_map_t<ScbRequestedVariant> result;
	if (value.IsNull()) {
		return result;
	}
	if (value.type().id() == LogicalTypeId::VARCHAR) {
		return ParseVariantsJSON(value.ToString());
	}
	if (value.type().id() == LogicalTypeId::MAP) {
		for (const auto &entry : MapValue::GetChildren(value)) {
			auto &kv = StructValue::GetChildren(entry);
			if (kv.size() != 2 || kv[0].IsNull() || kv[1].IsNull()) {
				continue;
			}
			result[kv[0].ToString()] = ParseRequestedVariantValue(kv[1]);
		}
		return result;
	}
	if (value.type().id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size() && i < child_types.size(); i++) {
			if (children[i].IsNull()) {
				continue;
			}
			result[child_types[i].first] = ParseRequestedVariantValue(children[i]);
		}
		return result;
	}
	throw InvalidInputException("SCB variants argument must be NULL, JSON text, MAP, or STRUCT");
}

static const ScbVariableMetadata *FindVariableMetadata(const ScbMetadataCacheEntry &metadata, const string &variable_code) {
	for (const auto &variable : metadata.variables) {
		if (variable.variable_code == variable_code) {
			return &variable;
		}
	}
	return nullptr;
}

static vector<ScbDimensionVariant> DeriveDimensionVariants(ClientContext &context, const ScbMetadataCacheEntry &metadata,
                                                           const ScbVariableMetadata &variable) {
	vector<ScbDimensionVariant> result;

	for (idx_t i = 0; i < variable.codelists.size(); i++) {
		const auto &codelist_ref = variable.codelists[i];
		shared_ptr<ScbCodelistCacheEntry> codelist;
		try {
			codelist = LoadCodelistEntry(context, codelist_ref.id, metadata.language);
		} catch (const IOException &) {
			try {
				codelist = ProbeCodelistEntryFromData(context, metadata, variable.variable_code, codelist_ref.id,
				                                      codelist_ref.label, codelist_ref.type);
			} catch (const Exception &) {
				continue;
			}
		}
		auto codelist_type =
		    StringUtil::Lower(codelist_ref.type.empty() ? codelist->type : codelist_ref.type);
		if (codelist_type != "valueset" && codelist_type != "aggregation") {
			continue;
		}
		ScbDimensionVariant variant;
		variant.codelist_id = codelist_ref.id;
		variant.variant_name = codelist_ref.label.empty() ? codelist->label : codelist_ref.label;
		variant.variant_kind = codelist_type;
		for (const auto &entry : codelist->values) {
			variant.values.push_back(MakeLabeledValue(entry.value_code, entry.label));
		}
		result.push_back(std::move(variant));
	}
	if (!result.empty()) {
		std::sort(result.begin(), result.end(), [](const ScbDimensionVariant &left, const ScbDimensionVariant &right) {
			if (left.values.size() != right.values.size()) {
				return left.values.size() < right.values.size();
			}
			return left.variant_name < right.variant_name;
		});
		return result;
	}

	bool has_hierarchy = false;
	unordered_map<string, idx_t> value_index;
	for (idx_t i = 0; i < variable.values.size(); i++) {
		value_index[variable.values[i].value_code] = i;
		if (!variable.values[i].parent_code.empty() || variable.values[i].child_count > 0) {
			has_hierarchy = true;
		}
	}
	if (has_hierarchy) {
		unordered_map<string, int64_t> depth_cache;
		for (const auto &value : variable.values) {
			string code = value.value_code;
			int64_t depth = 0;
			auto current = &value;
			while (current && !current->parent_code.empty()) {
				depth++;
				auto parent_it = value_index.find(current->parent_code);
				current = parent_it == value_index.end() ? nullptr : &variable.values[parent_it->second];
			}
			depth_cache[code] = depth;
		}
		map<int64_t, vector<string> > variant_map;
		for (const auto &value : variable.values) {
			variant_map[depth_cache[value.value_code]].push_back(value.value_code);
		}
		for (map<int64_t, vector<string> >::const_iterator it = variant_map.begin(); it != variant_map.end(); ++it) {
			ScbDimensionVariant variant;
			variant.variant_name = "depth " + to_string(it->first);
			variant.variant_kind = "hierarchy";
			for (const auto &value_code : it->second) {
				auto value_index_entry = value_index.find(value_code);
				if (value_index_entry == value_index.end()) {
					continue;
				}
				const auto &value = variable.values[value_index_entry->second];
				variant.values.push_back(MakeLabeledValue(value.value_code, value.label));
			}
			result.push_back(std::move(variant));
		}
		return result;
	}

	ScbDimensionVariant only_variant;
	only_variant.variant_name = "all";
	only_variant.variant_kind = "flat";
	for (const auto &value : variable.values) {
		only_variant.values.push_back(MakeLabeledValue(value.value_code, value.label));
	}
	result.push_back(std::move(only_variant));
	return result;
}

static shared_ptr<ScbDerivedVariantsCacheEntry> LoadDerivedVariantsEntry(ClientContext &context,
                                                                        const ScbMetadataCacheEntry &metadata) {
	auto identity = metadata.table_id + "|" + metadata.language;
	return GetOrLoadCacheEntry<ScbDerivedVariantsCacheEntry>(context, BuildCacheKey("dimension_variants", identity),
	                                                        [&]() {
		unordered_map<string, vector<ScbDimensionVariant>> variants_by_variable;
		for (const auto &variable : metadata.variables) {
			if (IsMetricRole(variable.role)) {
				continue;
			}
			auto variants = DeriveDimensionVariants(context, metadata, variable);
			variants_by_variable.emplace(variable.variable_code, std::move(variants));
		}
		return make_shared_ptr<ScbDerivedVariantsCacheEntry>(std::move(variants_by_variable));
	});
}

static optional_ptr<const ScbRequestedVariant>
FindRequestedVariant(const case_insensitive_map_t<ScbRequestedVariant> &requested_variants,
                     const ScbVariableMetadata &variable, case_insensitive_set_t &used_keys) {
	auto code_entry = requested_variants.find(variable.variable_code);
	if (code_entry != requested_variants.end()) {
		used_keys.insert(code_entry->first);
		return &code_entry->second;
	}
	auto label_entry = requested_variants.find(variable.label);
	if (label_entry != requested_variants.end()) {
		used_keys.insert(label_entry->first);
		return &label_entry->second;
	}
	return nullptr;
}

static idx_t ResolveRequestedVariant(const ScbVariableMetadata &variable, const vector<ScbDimensionVariant> &variants,
                                     const ScbRequestedVariant &requested_variant) {
	if (requested_variant.by_index) {
		if (requested_variant.variant_index < 0 || NumericCast<idx_t>(requested_variant.variant_index) >= variants.size()) {
			throw InvalidInputException("Requested variant %s=%lld is out of range [0, %llu]", variable.variable_code,
			                            static_cast<long long>(requested_variant.variant_index),
			                            static_cast<unsigned long long>(variants.size() - 1));
		}
		return NumericCast<idx_t>(requested_variant.variant_index);
	}
	for (idx_t i = 0; i < variants.size(); i++) {
		if (variants[i].variant_name == requested_variant.variant_name) {
			return i;
		}
	}
	for (idx_t i = 0; i < variants.size(); i++) {
		if (StringUtil::CIEquals(variants[i].variant_name, requested_variant.variant_name)) {
			return i;
		}
	}
	throw InvalidInputException("Unknown variant %s=%s", variable.variable_code, requested_variant.variant_name);
}

struct ScbScanPlan {
	vector<ScbSelectionItem> selection_items;
	vector<ScbOutputDimension> output_dimensions;
	string metric_variable_code;
	vector<ScbMetricColumn> metric_columns;
};

struct ScbJsonStatDimension {
	string variable_code;
	vector<ScbLabeledValue> values;
};

static void BuildScanSchema(const ScbScanPlan &plan, vector<LogicalType> &return_types, vector<string> &names);
static unique_ptr<ColumnDataCollection> MaterializeScanRows(ClientContext &context, const ScbScanPlan &plan,
                                                            yyjson_val *root);

static ScbScanPlan BuildScanPlan(ClientContext &context, const string &table_id, const Value &variants_value,
                                 const string &lang) {
	ScbScanPlan plan;
	auto metadata = LoadMetadataEntry(context, table_id, lang);
	auto derived_variants = LoadDerivedVariantsEntry(context, *metadata);
	auto requested_variants = ParseVariantsValue(variants_value);
	case_insensitive_set_t used_variant_keys;

	for (idx_t i = 0; i < metadata->variables.size(); i++) {
		const auto &variable = metadata->variables[i];
		if (IsMetricRole(variable.role)) {
			if (!plan.metric_variable_code.empty()) {
				throw InvalidInputException("SCB scan currently supports at most one metric dimension");
			}
			plan.metric_variable_code = variable.variable_code;
			ScbSelectionItem item;
			item.variable_code = variable.variable_code;
			for (const auto &value : variable.values) {
				item.value_codes.push_back(value.value_code);
				ScbMetricColumn metric_column;
				metric_column.value_code = value.value_code;
				metric_column.label = value.label;
				plan.metric_columns.push_back(std::move(metric_column));
			}
			plan.selection_items.push_back(std::move(item));
			continue;
		}

		auto variants_entry = derived_variants->variants_by_variable.find(variable.variable_code);
		if (variants_entry == derived_variants->variants_by_variable.end() || variants_entry->second.empty()) {
			throw InvalidInputException("Could not derive any variants for SCB dimension \"%s\"", variable.variable_code);
		}
		const auto &variants = variants_entry->second;
		idx_t selected_variant_index = 0;
		auto requested_variant = FindRequestedVariant(requested_variants, variable, used_variant_keys);
		if (requested_variant) {
			selected_variant_index = ResolveRequestedVariant(variable, variants, *requested_variant);
		}

		ScbSelectionItem item;
		item.variable_code = variable.variable_code;
		item.codelist = variants[selected_variant_index].codelist_id;
		for (const auto &value : variants[selected_variant_index].values) {
			item.value_codes.push_back(value.value_code);
		}
		plan.selection_items.push_back(std::move(item));
		ScbOutputDimension output_dimension;
		output_dimension.variable_code = variable.variable_code;
		output_dimension.label = variable.label;
		for (const auto &value : variants[selected_variant_index].values) {
			output_dimension.labels_by_code.emplace(value.value_code, value.label);
		}
		plan.output_dimensions.push_back(std::move(output_dimension));
	}

	for (case_insensitive_map_t<ScbRequestedVariant>::const_iterator it = requested_variants.begin();
	     it != requested_variants.end(); ++it) {
		if (used_variant_keys.find(it->first) == used_variant_keys.end()) {
			throw InvalidInputException("Unknown SCB variant dimension \"%s\"", it->first);
		}
	}

	return plan;
}

static shared_ptr<ScbMaterializedScanChunkCacheEntry>
LoadMaterializedScanChunk(ClientContext &context, const ScbScanPlan &plan, const string &table_id,
                          const string &selection_json, const string &lang) {
	auto cache_identity = StringUtil::Format("%s|%s|%s", table_id, lang, selection_json);
	return GetOrLoadCacheEntry<ScbMaterializedScanChunkCacheEntry>(
	    context, BuildCacheKey("scan_chunk_rows", cache_identity), [&]() {
		    auto data_url = StringUtil::Format("%s/tables/%s/data?lang=%s&outputFormat=json-stat2", GetScbBaseURL(),
		                                       StringUtil::URLEncode(table_id), StringUtil::URLEncode(lang));
		    auto response_body = HTTPPostJSON(context, data_url, selection_json, "application/json");
		    JSONDoc doc(response_body);
		    auto root = RequireObject(doc.Root(), "scan response");
		    auto rows = MaterializeScanRows(context, plan, root.get());
		    return make_shared_ptr<ScbMaterializedScanChunkCacheEntry>(std::move(rows));
	    });
}

static vector<string> ParseOrderedCategoryCodes(yyjson_val *index_value) {
	vector<string> result;
	if (!index_value) {
		return result;
	}
	if (yyjson_is_arr(index_value)) {
		yyjson_val *entry;
		size_t idx, max;
		yyjson_arr_foreach(index_value, idx, max, entry) {
			if (yyjson_is_str(entry)) {
				result.emplace_back(yyjson_get_str(entry));
			}
		}
		return result;
	}
	if (!yyjson_is_obj(index_value)) {
		throw InvalidInputException("Expected JSON-stat category.index to be an array or object");
	}
	vector<pair<int64_t, string>> ordered_codes;
	yyjson_val *code_key, *position;
	size_t index_idx, index_max;
	yyjson_obj_foreach(index_value, index_idx, index_max, code_key, position) {
		if ((!yyjson_is_int(position) && !yyjson_is_uint(position)) || !yyjson_is_str(code_key)) {
			continue;
		}
		ordered_codes.emplace_back(yyjson_get_sint(position), yyjson_get_str(code_key));
	}
	std::sort(ordered_codes.begin(), ordered_codes.end(),
	          [](const pair<int64_t, string> &left, const pair<int64_t, string> &right) { return left.first < right.first; });
	for (const auto &entry : ordered_codes) {
		result.push_back(entry.second);
	}
	return result;
}

static vector<idx_t> ParseSizeArray(yyjson_val *size_value) {
	vector<idx_t> result;
	auto size_array = RequireArray(size_value, "size");
	yyjson_val *entry;
	size_t idx, max;
	yyjson_arr_foreach(size_array.get(), idx, max, entry) {
		if (!yyjson_is_int(entry) && !yyjson_is_uint(entry)) {
			throw InvalidInputException("Expected JSON-stat size entries to be integers");
		}
		result.push_back(NumericCast<idx_t>(yyjson_get_sint(entry)));
	}
	return result;
}

static vector<string> ParseIdArray(yyjson_val *id_value) {
	vector<string> result;
	auto ids = RequireArray(id_value, "id");
	yyjson_val *entry;
	size_t idx, max;
	yyjson_arr_foreach(ids.get(), idx, max, entry) {
		if (!yyjson_is_str(entry)) {
			throw InvalidInputException("Expected JSON-stat id entries to be strings");
		}
		result.emplace_back(yyjson_get_str(entry));
	}
	return result;
}

static vector<ScbJsonStatDimension> ParseScanResponseDimensions(yyjson_val *root) {
	auto id_values = ParseIdArray(yyjson_obj_get(root, "id"));
	auto size_values = ParseSizeArray(yyjson_obj_get(root, "size"));
	if (id_values.size() != size_values.size()) {
		throw InvalidInputException("SCB JSON-stat response has mismatched id/size lengths");
	}

	auto dimension = RequireObject(yyjson_obj_get(root, "dimension"), "dimension");
	vector<ScbJsonStatDimension> result;
	result.reserve(id_values.size());

	for (idx_t dim_idx = 0; dim_idx < id_values.size(); dim_idx++) {
		const auto &variable_code = id_values[dim_idx];
		auto variable_node = RequireObject(yyjson_obj_get(dimension.get(), variable_code.c_str()), "dimension entry");
		auto category = RequireObject(yyjson_obj_get(variable_node.get(), "category"), "dimension category");
		auto codes = ParseOrderedCategoryCodes(yyjson_obj_get(category.get(), "index"));
		auto label_object = yyjson_obj_get(category.get(), "label");

		ScbJsonStatDimension dimension_row;
		dimension_row.variable_code = variable_code;
		for (const auto &code : codes) {
			string label = code;
			if (label_object && yyjson_is_obj(label_object)) {
				auto label_value = yyjson_obj_get(label_object, code.c_str());
				if (label_value && yyjson_is_str(label_value)) {
					label = yyjson_get_str(label_value);
				}
			}
			dimension_row.values.push_back({code, std::move(label)});
		}
		if (dimension_row.values.size() != size_values[dim_idx]) {
			throw InvalidInputException("SCB JSON-stat response has mismatched category.index and size for \"%s\"",
			                            variable_code);
		}
		result.push_back(std::move(dimension_row));
	}
	return result;
}

static yyjson_val *GetIndexedJSONStatEntry(yyjson_val *node, idx_t index) {
	if (!node || yyjson_is_null(node)) {
		return nullptr;
	}
	if (yyjson_is_arr(node)) {
		return yyjson_arr_get(node, index);
	}
	if (yyjson_is_obj(node)) {
		return yyjson_obj_get(node, to_string(index).c_str());
	}
	return nullptr;
}

static bool HasStatusSymbol(yyjson_val *status_entry) {
	return status_entry && yyjson_is_str(status_entry) && strlen(yyjson_get_str(status_entry)) > 0;
}

static Value GetNumericCellValue(yyjson_val *value_entry) {
	if (!value_entry || yyjson_is_null(value_entry)) {
		return Value();
	}
	if (yyjson_is_real(value_entry)) {
		return Value(yyjson_get_real(value_entry));
	}
	if (yyjson_is_int(value_entry) || yyjson_is_uint(value_entry)) {
		return Value(static_cast<double>(yyjson_get_num(value_entry)));
	}
	throw InvalidInputException("Expected JSON-stat values to be numeric or null");
}

static vector<idx_t> ComputeDimensionStrides(const vector<ScbJsonStatDimension> &dimensions) {
	vector<idx_t> strides(dimensions.size(), 1);
	if (dimensions.empty()) {
		return strides;
	}
	for (idx_t i = dimensions.size(); i-- > 0;) {
		if (i + 1 < dimensions.size()) {
			strides[i] = strides[i + 1] * MaxValue<idx_t>(dimensions[i + 1].values.size(), 1);
		}
	}
	return strides;
}

static idx_t ComputeFlatIndex(const vector<idx_t> &indexes, const vector<idx_t> &strides) {
	idx_t flat_index = 0;
	for (idx_t i = 0; i < indexes.size(); i++) {
		flat_index += indexes[i] * strides[i];
	}
	return flat_index;
}

static idx_t CountScanRows(const vector<ScbJsonStatDimension> &dimensions, optional_idx metric_dimension_index) {
	idx_t row_count = 1;
	for (idx_t i = 0; i < dimensions.size(); i++) {
		if (metric_dimension_index.IsValid() && metric_dimension_index.GetIndex() == i) {
			continue;
		}
		row_count *= MaxValue<idx_t>(dimensions[i].values.size(), 1);
	}
	return row_count;
}

static idx_t CountSelectionCells(const vector<ScbSelectionItem> &selection_items) {
	idx_t total = 1;
	for (const auto &item : selection_items) {
		total *= MaxValue<idx_t>(item.value_codes.size(), 1);
	}
	return total;
}

static void BuildSelectionChunksRecursive(const vector<ScbSelectionItem> &selection_items, const string &metric_variable_code,
                                          idx_t max_cells, vector<vector<ScbSelectionItem>> &result);

static optional_ptr<ScbSelectionItem> FindSelectionItem(vector<ScbSelectionItem> &selection_items, const string &variable_code) {
	for (auto &item : selection_items) {
		if (item.variable_code == variable_code) {
			return &item;
		}
	}
	return nullptr;
}

static bool EvaluateDimensionFilter(const TableFilter &filter, const Value &label);

static bool EvaluateDimensionFilterList(const vector<unique_ptr<TableFilter>> &filters, const Value &label,
                                        bool conjunction_and) {
	for (const auto &child : filters) {
		auto match = EvaluateDimensionFilter(*child, label);
		if (conjunction_and) {
			if (!match) {
				return false;
			}
		} else if (match) {
			return true;
		}
	}
	return conjunction_and;
}

static bool EvaluateDimensionFilter(const TableFilter &filter, const Value &label) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		return filter.Cast<ConstantFilter>().Compare(label);
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		for (const auto &candidate : in_filter.values) {
			if (label == candidate.DefaultCastAs(LogicalType::VARCHAR)) {
				return true;
			}
		}
		return false;
	}
	case TableFilterType::CONJUNCTION_AND:
		return EvaluateDimensionFilterList(filter.Cast<ConjunctionAndFilter>().child_filters, label, true);
	case TableFilterType::CONJUNCTION_OR:
		return EvaluateDimensionFilterList(filter.Cast<ConjunctionOrFilter>().child_filters, label, false);
	case TableFilterType::IS_NULL:
		return label.IsNull();
	case TableFilterType::IS_NOT_NULL:
		return !label.IsNull();
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			return true;
		}
		return EvaluateDimensionFilter(*optional_filter.child_filter, label);
	}
	default:
		throw NotImplementedException("SCB scan filter pushdown encountered an unsupported filter type");
	}
}

static void ApplyDimensionFilterPushdown(vector<ScbSelectionItem> &selection_items, const ScbScanPlan &plan,
                                         const vector<column_t> &column_ids, const TableFilterSet &filters) {
	for (const auto &entry : filters.filters) {
		// TableFilterSet keys are relative to the scan columns DuckDB asked us to read, not to the full
		// table-function schema. Map through column_ids first, then check whether the filter targets a
		// pushable SCB dimension column.
		const auto scan_column_index = entry.first;
		if (scan_column_index >= column_ids.size()) {
			continue;
		}
		const auto schema_column_index = column_ids[scan_column_index];
		if (schema_column_index >= plan.output_dimensions.size()) {
			continue;
		}
		const auto &dimension = plan.output_dimensions[schema_column_index];
		auto selection_item = FindSelectionItem(selection_items, dimension.variable_code);
		if (!selection_item) {
			throw IOException("SCB scan selection is missing dimension \"%s\"", dimension.variable_code);
		}

		vector<string> filtered_codes;
		filtered_codes.reserve(selection_item->value_codes.size());
		for (const auto &value_code : selection_item->value_codes) {
			auto label_entry = dimension.labels_by_code.find(value_code);
			auto label = label_entry == dimension.labels_by_code.end() ? value_code : label_entry->second;
			if (EvaluateDimensionFilter(*entry.second, Value(label))) {
				filtered_codes.push_back(value_code);
			}
		}
		selection_item->value_codes = std::move(filtered_codes);
	}
}

static vector<string> BuildSelectionJSONChunks(const vector<ScbSelectionItem> &selection_items,
                                               const string &metric_variable_code, idx_t max_cells) {
	for (const auto &item : selection_items) {
		if (item.value_codes.empty()) {
			return {};
		}
	}
	vector<vector<ScbSelectionItem>> selection_chunks;
	BuildSelectionChunksRecursive(selection_items, metric_variable_code, max_cells, selection_chunks);
	vector<string> selection_json_chunks;
	selection_json_chunks.reserve(selection_chunks.size());
	for (const auto &selection_chunk : selection_chunks) {
		selection_json_chunks.push_back(SelectionItemsToJSON(selection_chunk));
	}
	return selection_json_chunks;
}

static void BuildSelectionChunksRecursive(const vector<ScbSelectionItem> &selection_items,
                                          const string &metric_variable_code, idx_t max_cells,
                                          vector<vector<ScbSelectionItem>> &result) {
	if (CountSelectionCells(selection_items) <= max_cells) {
		result.push_back(selection_items);
		return;
	}

	optional_idx split_index;
	idx_t split_size = 0;
	for (idx_t i = 0; i < selection_items.size(); i++) {
		if (!metric_variable_code.empty() && selection_items[i].variable_code == metric_variable_code) {
			continue;
		}
		if (selection_items[i].value_codes.size() > split_size) {
			split_size = selection_items[i].value_codes.size();
			split_index = i;
		}
	}
	if (!split_index.IsValid() || split_size <= 1) {
		throw IOException("SCB scan exceeds maxDataCells and could not be split safely");
	}

	auto left = selection_items;
	auto right = selection_items;
	auto mid = split_size / 2;
	left[split_index.GetIndex()].value_codes.assign(selection_items[split_index.GetIndex()].value_codes.begin(),
	                                                selection_items[split_index.GetIndex()].value_codes.begin() + mid);
	right[split_index.GetIndex()].value_codes.assign(selection_items[split_index.GetIndex()].value_codes.begin() + mid,
	                                                 selection_items[split_index.GetIndex()].value_codes.end());
	BuildSelectionChunksRecursive(left, metric_variable_code, max_cells, result);
	BuildSelectionChunksRecursive(right, metric_variable_code, max_cells, result);
}

static void BuildScanSchema(const ScbScanPlan &plan, vector<LogicalType> &return_types, vector<string> &names) {
	for (const auto &dimension : plan.output_dimensions) {
		names.push_back(dimension.label.empty() ? dimension.variable_code : dimension.label);
		return_types.push_back(LogicalType::VARCHAR);
	}
	if (!plan.metric_variable_code.empty()) {
		for (const auto &metric_column : plan.metric_columns) {
			names.push_back(metric_column.label.empty() ? metric_column.value_code : metric_column.label);
			return_types.push_back(LogicalType::DOUBLE);
		}
	} else {
		names.push_back("value");
		return_types.push_back(LogicalType::DOUBLE);
	}
	QueryResult::DeduplicateColumns(names);
}

static unique_ptr<ColumnDataCollection> MaterializeScanRows(ClientContext &context, const ScbScanPlan &plan,
                                                            yyjson_val *root) {
	auto dimensions = ParseScanResponseDimensions(root);
	vector<idx_t> strides = ComputeDimensionStrides(dimensions);
	vector<LogicalType> types;
	vector<string> names;
	BuildScanSchema(plan, types, names);
	auto rows = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	rows->InitializeAppend(append_state);
	DataChunk append_chunk;
	append_chunk.Initialize(context, types);
	auto flush_chunk = [&]() {
		if (append_chunk.size() == 0) {
			return;
		}
		rows->Append(append_state, append_chunk);
		append_chunk.Reset();
	};

	optional_idx metric_dimension_index;
	for (idx_t i = 0; i < dimensions.size(); i++) {
		if (!plan.metric_variable_code.empty() && dimensions[i].variable_code == plan.metric_variable_code) {
			if (metric_dimension_index.IsValid()) {
				throw InvalidInputException("SCB scan currently supports at most one metric dimension");
			}
			metric_dimension_index = i;
		}
	}

	unordered_map<string, idx_t> dimension_index_by_code;
	for (idx_t i = 0; i < dimensions.size(); i++) {
		dimension_index_by_code[dimensions[i].variable_code] = i;
	}
	vector<idx_t> output_dimension_indexes;
	output_dimension_indexes.reserve(plan.output_dimensions.size());
	for (const auto &dimension : plan.output_dimensions) {
		auto entry = dimension_index_by_code.find(dimension.variable_code);
		if (entry == dimension_index_by_code.end()) {
			throw IOException("SCB scan response is missing dimension \"%s\"", dimension.variable_code);
		}
		output_dimension_indexes.push_back(entry->second);
	}

	auto value_node = yyjson_obj_get(root, "value");
	auto status_node = yyjson_obj_get(root, "status");
	idx_t row_count = CountScanRows(dimensions, metric_dimension_index);

	for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
		vector<idx_t> indexes(dimensions.size(), 0);
		idx_t remainder = row_idx;
		for (idx_t dim_idx = dimensions.size(); dim_idx-- > 0;) {
			if (metric_dimension_index.IsValid() && metric_dimension_index.GetIndex() == dim_idx) {
				continue;
			}
			auto dim_size = MaxValue<idx_t>(dimensions[dim_idx].values.size(), 1);
			indexes[dim_idx] = remainder % dim_size;
			remainder /= dim_size;
		}

		auto append_row = append_chunk.size();
		if (append_row == STANDARD_VECTOR_SIZE) {
			flush_chunk();
			append_row = 0;
		}

		idx_t output_col_idx = 0;
		for (idx_t non_metric_idx = 0; non_metric_idx < output_dimension_indexes.size(); non_metric_idx++) {
			const auto dim_idx = output_dimension_indexes[non_metric_idx];
			const auto &dimension = dimensions[dim_idx];
			auto value_idx = indexes[dim_idx];
			if (value_idx >= dimension.values.size()) {
				append_chunk.SetValue(output_col_idx++, append_row, Value());
			} else {
				const auto &value = dimension.values[value_idx];
				const auto &output_dimension = plan.output_dimensions[non_metric_idx];
				auto label_entry = output_dimension.labels_by_code.find(value.value_code);
				auto label = label_entry == output_dimension.labels_by_code.end() ? value.label : label_entry->second;
				append_chunk.SetValue(output_col_idx++, append_row, Value(label));
			}
		}

		if (metric_dimension_index.IsValid()) {
			const auto metric_idx = metric_dimension_index.GetIndex();
			const auto &metric_dimension = dimensions[metric_idx];
			for (idx_t metric_value_idx = 0; metric_value_idx < metric_dimension.values.size(); metric_value_idx++) {
				indexes[metric_idx] = metric_value_idx;
				auto flat_index = ComputeFlatIndex(indexes, strides);
				auto status_entry = GetIndexedJSONStatEntry(status_node, flat_index);
				if (HasStatusSymbol(status_entry)) {
					append_chunk.SetValue(output_col_idx++, append_row, Value());
					continue;
				}
				append_chunk.SetValue(output_col_idx++, append_row,
				                      GetNumericCellValue(GetIndexedJSONStatEntry(value_node, flat_index)));
			}
		} else {
			auto flat_index = ComputeFlatIndex(indexes, strides);
			auto status_entry = GetIndexedJSONStatEntry(status_node, flat_index);
			if (HasStatusSymbol(status_entry)) {
				append_chunk.SetValue(output_col_idx, append_row, Value());
			} else {
				append_chunk.SetValue(output_col_idx, append_row,
				                      GetNumericCellValue(GetIndexedJSONStatEntry(value_node, flat_index)));
			}
			output_col_idx++;
		}
		append_chunk.SetCardinality(append_row + 1);
	}

	flush_chunk();
	return rows;
}

struct ScbScanBindData : public FunctionData {
	ScbScanBindData(string table_id_p, string lang_p, ScbScanPlan plan_p)
	    : table_id(std::move(table_id_p)), lang(std::move(lang_p)), plan(std::move(plan_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ScbScanBindData>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		return this == &other;
	}

	string table_id;
	string lang;
	ScbScanPlan plan;
};

struct SimpleOffsetGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

struct ScbScanGlobalState : public GlobalTableFunctionState {
	idx_t chunk_index = 0;
	vector<column_t> column_ids;
	vector<idx_t> projection_ids;
	vector<string> selection_json_chunks;
	shared_ptr<ScbMaterializedScanChunkCacheEntry> current_chunk;
	ColumnDataScanState scan_state;
	DataChunk scan_chunk;
	DataChunk all_columns;
	idx_t scan_chunk_offset = 0;
};

template <class ROW>
struct RowBindData : public FunctionData {
	explicit RowBindData(vector<ROW> rows_p) : rows(std::move(rows_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RowBindData<ROW>>(*this);
	}

	bool Equals(const FunctionData &other) const override {
		return this == &other;
	}

	vector<ROW> rows;
};

static unique_ptr<GlobalTableFunctionState> InitSimpleOffsetState(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SimpleOffsetGlobalState>();
}

static unique_ptr<GlobalTableFunctionState> InitScbScanState(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ScbScanBindData>();
	auto state = make_uniq<ScbScanGlobalState>();
	state->column_ids = input.column_ids;
	if (input.CanRemoveFilterColumns()) {
		state->projection_ids = input.projection_ids;
		vector<LogicalType> scan_types;
		vector<string> scan_names;
		BuildScanSchema(bind_data.plan, scan_types, scan_names);
		vector<LogicalType> all_column_types;
		all_column_types.reserve(input.column_ids.size());
		for (const auto &column_id : input.column_ids) {
			all_column_types.push_back(scan_types[column_id]);
		}
		state->all_columns.Initialize(context, all_column_types);
	}
	auto selection_items = bind_data.plan.selection_items;
	if (input.filters) {
		ApplyDimensionFilterPushdown(selection_items, bind_data.plan, input.column_ids, *input.filters);
	}

	auto max_cells = LoadConfigEntry(context)->row.max_data_cells;
	if (max_cells <= 0) {
		max_cells = 150000;
	}
	state->selection_json_chunks =
	    BuildSelectionJSONChunks(selection_items, bind_data.plan.metric_variable_code, NumericCast<idx_t>(max_cells));
	return state;
}

static void ProjectScanChunk(const vector<column_t> &column_ids, const vector<idx_t> &projection_ids,
                             const DataChunk &source, DataChunk &all_columns, idx_t offset, idx_t count,
                             DataChunk &output) {
	output.Reset();
	if (column_ids.empty() && projection_ids.empty()) {
		output.SetCardinality(count);
		return;
	}
	const bool remove_filter_columns = !projection_ids.empty();
	// source uses the full natural scan schema. column_ids selects the columns DuckDB asked us to scan from that
	// schema. projection_ids, when present, is relative to column_ids and tells us which scanned columns are still
	// needed after residual filters have been evaluated by DuckDB.
	if (remove_filter_columns) {
		all_columns.Reset();
	}
	if (offset == 0 && count == source.size()) {
		if (remove_filter_columns) {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				all_columns.data[i].Reference(source.data[column_ids[i]]);
			}
			all_columns.SetCardinality(count);
			output.ReferenceColumns(all_columns, projection_ids);
		} else {
			for (idx_t i = 0; i < column_ids.size(); i++) {
				output.data[i].Reference(source.data[column_ids[i]]);
			}
			output.SetCardinality(count);
		}
		return;
	}
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, UnsafeNumericCast<sel_t>(offset + i));
	}
	if (remove_filter_columns) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			all_columns.data[i].Slice(source.data[column_ids[i]], sel, count);
		}
		all_columns.SetCardinality(count);
		output.ReferenceColumns(all_columns, projection_ids);
	} else {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			output.data[i].Slice(source.data[column_ids[i]], sel, count);
		}
		output.SetCardinality(count);
	}
}

static bool TryGetNamedString(const named_parameter_map_t &parameters, const string &name, string &result) {
	auto entry = parameters.find(name);
	if (entry == parameters.end() || entry->second.IsNull()) {
		return false;
	}
	result = entry->second.ToString();
	return true;
}

static string GetNamedStringOrDefault(const named_parameter_map_t &parameters, const string &name,
                                      const string &default_value) {
	string result;
	return TryGetNamedString(parameters, name, result) ? result : default_value;
}

static bool GetNamedBoolOrDefault(const named_parameter_map_t &parameters, const string &name, bool default_value) {
	auto entry = parameters.find(name);
	if (entry == parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return BooleanValue::Get(entry->second);
}

static optional_idx GetNamedIndex(const named_parameter_map_t &parameters, const string &name) {
	auto entry = parameters.find(name);
	if (entry == parameters.end() || entry->second.IsNull()) {
		return optional_idx();
	}
	return NumericCast<idx_t>(entry->second.GetValue<int64_t>());
}

static unique_ptr<FunctionData> ScbConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names = {"api_version", "app_version", "default_language", "languages", "max_data_cells",
	         "max_calls_per_time_window", "time_window", "default_data_format", "data_formats"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::BIGINT,
	                LogicalType::BIGINT,
	                LogicalType::BIGINT,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR)};

	vector<ScbConfigRow> rows;
	rows.push_back(LoadConfigEntry(context)->row);
	return make_uniq<RowBindData<ScbConfigRow>>(std::move(rows));
}

static void ScbConfigFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<RowBindData<ScbConfigRow>>();
	auto &state = data_p.global_state->Cast<SimpleOffsetGlobalState>();
	if (state.offset >= data.rows.size()) {
		return;
	}
	const auto &row = data.rows[state.offset++];
	output.SetValue(0, 0, row.api_version);
	output.SetValue(1, 0, row.app_version);
	output.SetValue(2, 0, row.default_language);
	output.SetValue(3, 0, Value::LIST(LogicalType::VARCHAR, ToValueList(row.languages)));
	output.SetValue(4, 0, Value::BIGINT(row.max_data_cells));
	output.SetValue(5, 0, Value::BIGINT(row.max_calls_per_time_window));
	output.SetValue(6, 0, Value::BIGINT(row.time_window));
	output.SetValue(7, 0, row.default_data_format);
	output.SetValue(8, 0, Value::LIST(LogicalType::VARCHAR, ToValueList(row.data_formats)));
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> ScbTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names = {"language", "table_id", "label", "description", "updated", "first_period", "last_period",
	         "discontinued", "source", "subject_code", "time_unit", "category", "path_labels", "path_ids"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR};

	const auto lang = GetNamedStringOrDefault(input.named_parameters, "lang", "en");
	const auto query = GetNamedStringOrDefault(input.named_parameters, "query", "");
	const auto include_discontinued =
	    GetNamedBoolOrDefault(input.named_parameters, "include_discontinued", false);
	const auto past_days = GetNamedIndex(input.named_parameters, "past_days");

	auto entry = LoadTablesEntry(context, lang, query, include_discontinued, past_days);
	return make_uniq<RowBindData<ScbTableRow>>(entry->rows);
}

static void ScbTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<RowBindData<ScbTableRow>>();
	auto &state = data_p.global_state->Cast<SimpleOffsetGlobalState>();
	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[state.offset++];
		output.SetValue(0, count, row.language);
		output.SetValue(1, count, row.table_id);
		output.SetValue(2, count, row.label);
		output.SetValue(3, count, row.description);
		output.SetValue(4, count, row.updated);
		output.SetValue(5, count, row.first_period);
		output.SetValue(6, count, row.last_period);
		output.SetValue(7, count, row.discontinued);
		output.SetValue(8, count, row.source);
		output.SetValue(9, count, row.subject_code);
		output.SetValue(10, count, row.time_unit);
		output.SetValue(11, count, row.category);
		output.SetValue(12, count, row.path_labels);
		output.SetValue(13, count, row.path_ids);
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> ScbVariablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names = {"table_id", "language", "variable_code", "role", "variant", "variant_name", "variant_kind",
	         "member_count"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};

	if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
		throw BinderException("scb_variables(table_id) requires a non-null table_id");
	}
	const auto table_id = input.inputs[0].ToString();
	const auto lang = GetNamedStringOrDefault(input.named_parameters, "lang", "en");
	auto metadata = LoadMetadataEntry(context, table_id, lang);
	auto variants = LoadDerivedVariantsEntry(context, *metadata);
	vector<ScbVariableRow> rows;
	for (const auto &variable : metadata->variables) {
		if (IsMetricRole(variable.role)) {
			continue;
		}
		auto entry = variants->variants_by_variable.find(variable.variable_code);
		if (entry == variants->variants_by_variable.end()) {
			continue;
		}
		for (idx_t variant_idx = 0; variant_idx < entry->second.size(); variant_idx++) {
			const auto &variant = entry->second[variant_idx];
			ScbVariableRow row;
			row.table_id = table_id;
			row.language = lang;
			row.variable_code = variable.variable_code;
			row.role = variable.role;
			row.variant = NumericCast<int64_t>(variant_idx);
			row.variant_name = variant.variant_name;
			row.variant_kind = variant.variant_kind;
			row.member_count = NumericCast<int64_t>(variant.values.size());
			rows.push_back(std::move(row));
		}
	}
	return make_uniq<RowBindData<ScbVariableRow>>(std::move(rows));
}

static void ScbVariablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<RowBindData<ScbVariableRow>>();
	auto &state = data_p.global_state->Cast<SimpleOffsetGlobalState>();
	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[state.offset++];
		output.SetValue(0, count, row.table_id);
		output.SetValue(1, count, row.language);
		output.SetValue(2, count, row.variable_code);
		output.SetValue(3, count, row.role);
		output.SetValue(4, count, Value::BIGINT(row.variant));
		output.SetValue(5, count, row.variant_name);
		output.SetValue(6, count, row.variant_kind);
		output.SetValue(7, count, Value::BIGINT(row.member_count));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> ScbValuesBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names = {"table_id", "language", "variable_code", "value_code", "label", "sort_index",
	         "parent_code", "child_count", "is_leaf"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT,
	                LogicalType::BOOLEAN};

	if (input.inputs.size() != 2 || input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("scb_values(table_id, variable_code) requires non-null arguments");
	}
	const auto table_id = input.inputs[0].ToString();
	const auto variable_code = input.inputs[1].ToString();
	const auto lang = GetNamedStringOrDefault(input.named_parameters, "lang", "en");
	auto entry = LoadMetadataEntry(context, table_id, lang);
	auto variable = FindVariableMetadata(*entry, variable_code);
	vector<ScbValueRow> filtered;
	if (variable) {
		filtered.reserve(variable->values.size());
		for (const auto &value : variable->values) {
			ScbValueRow row;
			row.table_id = table_id;
			row.language = lang;
			row.variable_code = variable_code;
			row.value_code = value.value_code;
			row.label = value.label;
			row.sort_index = value.sort_index;
			row.parent_code = value.parent_code;
			row.child_count = value.child_count;
			filtered.push_back(std::move(row));
		}
	}
	return make_uniq<RowBindData<ScbValueRow>>(std::move(filtered));
}

static void ScbValuesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<RowBindData<ScbValueRow>>();
	auto &state = data_p.global_state->Cast<SimpleOffsetGlobalState>();
	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		const auto &row = data.rows[state.offset++];
		output.SetValue(0, count, row.table_id);
		output.SetValue(1, count, row.language);
		output.SetValue(2, count, row.variable_code);
		output.SetValue(3, count, row.value_code);
		output.SetValue(4, count, row.label);
		output.SetValue(5, count, Value::BIGINT(row.sort_index));
		output.SetValue(6, count, row.parent_code);
		output.SetValue(7, count, Value::BIGINT(row.child_count));
		output.SetValue(8, count, row.child_count == 0);
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> ScbScanBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty() || input.inputs.size() > 3 || input.inputs[0].IsNull()) {
		throw BinderException("scb_scan(table_id [, variants [, lang]]) requires a non-null table_id");
	}

	auto table_id = input.inputs[0].ToString();
	Value variants_value(LogicalType::VARCHAR);
	string lang = "en";
	if (input.inputs.size() >= 2) {
		variants_value = input.inputs[1];
	}
	if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
		lang = input.inputs[2].ToString();
	}

	auto plan = BuildScanPlan(context, table_id, variants_value, lang);
	BuildScanSchema(plan, return_types, names);
	return make_uniq<ScbScanBindData>(table_id, lang, std::move(plan));
}

static void ScbScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ScbScanBindData>();
	auto &state = data_p.global_state->Cast<ScbScanGlobalState>();
	while (true) {
		if (!state.current_chunk) {
			if (state.chunk_index >= state.selection_json_chunks.size()) {
				return;
			}
			state.current_chunk = LoadMaterializedScanChunk(context, data.plan, data.table_id,
			                                               state.selection_json_chunks[state.chunk_index++], data.lang);
			state.current_chunk->rows->InitializeScan(state.scan_state);
			if (state.scan_chunk.data.empty()) {
				state.current_chunk->rows->InitializeScanChunk(state.scan_state, state.scan_chunk);
			} else {
				state.scan_chunk.Reset();
			}
			state.scan_chunk_offset = 0;
		}
		if (state.scan_chunk_offset >= state.scan_chunk.size()) {
			if (!state.current_chunk->rows->Scan(state.scan_state, state.scan_chunk)) {
				state.current_chunk.reset();
				continue;
			}
			state.scan_chunk_offset = 0;
		}
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.scan_chunk.size() - state.scan_chunk_offset);
		ProjectScanChunk(state.column_ids, state.projection_ids, state.scan_chunk, state.all_columns,
		                 state.scan_chunk_offset, count, output);
		state.scan_chunk_offset += count;
		return;
	}
}

static bool ScbScanSupportsPushdownType(const FunctionData &bind_data_p, idx_t column_index) {
	auto &bind_data = bind_data_p.Cast<ScbScanBindData>();
	return column_index < bind_data.plan.output_dimensions.size();
}

static TableFunction MakeScbScanFunction(const string &name, const vector<LogicalType> &arguments) {
	auto function = TableFunction(name, arguments, ScbScanFunction, ScbScanBind, InitScbScanState);
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.supports_pushdown_type = ScbScanSupportsPushdownType;
	return function;
}

static TableFunctionSet MakeScbScanFunctionSet(const string &name) {
	TableFunctionSet result(name);
	result.AddFunction(MakeScbScanFunction(name, {LogicalType::VARCHAR}));
	result.AddFunction(MakeScbScanFunction(name, {LogicalType::VARCHAR, LogicalType::ANY}));
	result.AddFunction(MakeScbScanFunction(name, {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::VARCHAR}));
	return result;
}

static void RegisterFunctions(ExtensionLoader &loader) {
	auto scb_config = TableFunction("scb_config", {}, ScbConfigFunction, ScbConfigBind, InitSimpleOffsetState);
	loader.RegisterFunction(scb_config);

	auto scb_tables = TableFunction("scb_tables", {}, ScbTablesFunction, ScbTablesBind, InitSimpleOffsetState);
	scb_tables.named_parameters["lang"] = LogicalType::VARCHAR;
	scb_tables.named_parameters["query"] = LogicalType::VARCHAR;
	scb_tables.named_parameters["include_discontinued"] = LogicalType::BOOLEAN;
	scb_tables.named_parameters["past_days"] = LogicalType::BIGINT;
	loader.RegisterFunction(scb_tables);

	auto scb_variables =
	    TableFunction("scb_variables", {LogicalType::VARCHAR}, ScbVariablesFunction, ScbVariablesBind,
	                  InitSimpleOffsetState);
	scb_variables.named_parameters["lang"] = LogicalType::VARCHAR;
	loader.RegisterFunction(scb_variables);

	auto scb_values = TableFunction("scb_values", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScbValuesFunction,
	                                ScbValuesBind, InitSimpleOffsetState);
	scb_values.named_parameters["lang"] = LogicalType::VARCHAR;
	loader.RegisterFunction(scb_values);

	loader.RegisterFunction(MakeScbScanFunctionSet("scb_scan"));
	loader.RegisterFunction(MakeScbScanFunctionSet("scb_table"));
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("SCB Statistics Sweden API integration with DuckDB-native metadata and scan caching");
	RegisterFunctions(loader);
}

} // namespace

void ScbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ScbExtension::Name() {
	return "scb";
}

std::string ScbExtension::Version() const {
#ifdef EXT_VERSION_SCB
	return EXT_VERSION_SCB;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(scb, loader) {
	duckdb::LoadInternal(loader);
}
}
