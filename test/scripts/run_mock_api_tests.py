#!/usr/bin/env python3

import json
import os
import socket
import subprocess
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
DUCKDB_BIN = ROOT / "build" / "release" / "duckdb"


TABLES = {
    "MOCK_MAIN": {
        "id": "MOCK_MAIN",
        "label": {"en": "Mock metric table", "sv": "Mock metriktabell"},
        "description": {"en": "Deterministic scan fixture", "sv": "Deterministisk skanningsfixtur"},
        "updated": "2026-03-01",
        "firstPeriod": "2024M01",
        "lastPeriod": "2024M02",
        "discontinued": False,
        "source": "Mock source",
        "subjectCode": "SUBJ",
        "timeUnit": "month",
        "category": "demo",
        "paths": [[{"id": "demo", "label": "Demo"}]],
        "ids": ["Region", "Sex", "ContentsCode", "Tid"],
        "roles": {"geo": ["Region"], "metric": ["ContentsCode"], "time": ["Tid"]},
        "dimensions": {
            "Region": {
                "label": {"en": "region", "sv": "region"},
                "values": [
                    {"code": "00", "label": {"en": "Sweden", "sv": "Sverige"}},
                    {"code": "01", "label": {"en": "County A", "sv": "Lan A"}},
                    {"code": "03", "label": {"en": "County B", "sv": "Lan B"}},
                ],
                "extension": {
                    "codeLists": [
                        {"id": "vs_region_country", "label": {"en": "country", "sv": "land"}, "type": "Valueset"},
                        {"id": "vs_region_country_alias", "label": {"en": "country alias", "sv": "landsalias"}, "type": "Valueset"},
                        {"id": "vs_region_county", "label": {"en": "county", "sv": "lan"}, "type": "Valueset"},
                        {"id": "agg_region_county_grouped", "label": {"en": "county grouped", "sv": "lan grupperad"}, "type": "Aggregation"},
                        {"id": "vs_region_untyped", "label": {"en": "county alias", "sv": "lansalias"}},
                    ],
                },
            },
            "Sex": {
                "label": {"en": "sex", "sv": "kon"},
                "values": [
                    {"code": "T", "label": {"en": "total", "sv": "totalt"}},
                    {"code": "M", "label": {"en": "men", "sv": "man"}},
                    {"code": "F", "label": {"en": "women", "sv": "kvinnor"}},
                ],
                "extension": {},
            },
            "ContentsCode": {
                "label": {"en": "measure", "sv": "matt"},
                "values": [
                    {"code": "POP", "label": {"en": "Population", "sv": "Befolkning"}},
                    {"code": "INC", "label": {"en": "Population change", "sv": "Befolkningsforandring"}},
                ],
                "extension": {},
            },
            "Tid": {
                "label": {"en": "month", "sv": "manad"},
                "values": [
                    {"code": "2024M01", "label": {"en": "2024M01", "sv": "2024M01"}},
                    {"code": "2024M02", "label": {"en": "2024M02", "sv": "2024M02"}},
                ],
                "extension": {},
            },
        },
    },
    "MOCK_TREE": {
        "id": "MOCK_TREE",
        "label": {"en": "Mock hierarchy table", "sv": "Mock hierarkitabell"},
        "description": {"en": "Hierarchy fixture", "sv": "Hierarkifixtur"},
        "updated": "2026-03-02",
        "firstPeriod": "2024",
        "lastPeriod": "2024",
        "discontinued": False,
        "source": "Mock source",
        "subjectCode": "SUBJ",
        "timeUnit": "year",
        "category": "demo",
        "paths": [[{"id": "demo", "label": "Demo"}]],
        "ids": ["Age", "ContentsCode", "Tid"],
        "roles": {"metric": ["ContentsCode"], "time": ["Tid"]},
        "dimensions": {
            "Age": {
                "label": {"en": "age", "sv": "alder"},
                "values": [
                    {"code": "TOT", "label": {"en": "total", "sv": "totalt"}},
                    {"code": "ADULT", "label": {"en": "adult", "sv": "vuxen"}},
                    {"code": "CHILD", "label": {"en": "child", "sv": "barn"}},
                ],
                "child": {"TOT": ["ADULT", "CHILD"]},
                "extension": {},
            },
            "ContentsCode": {
                "label": {"en": "measure", "sv": "matt"},
                "values": [{"code": "OBS", "label": {"en": "Observation", "sv": "Observation"}}],
                "extension": {},
            },
            "Tid": {
                "label": {"en": "year", "sv": "ar"},
                "values": [{"code": "2024", "label": {"en": "2024", "sv": "2024"}}],
                "extension": {},
            },
        },
    },
    "MOCK_PROBE": {
        "id": "MOCK_PROBE",
        "label": {"en": "Mock probe table", "sv": "Mock probertabell"},
        "description": {"en": "Broken codelist probe fixture", "sv": "Trasig kodlista-fixtur"},
        "updated": "2026-03-03",
        "firstPeriod": "2024",
        "lastPeriod": "2024",
        "discontinued": True,
        "source": "Mock source",
        "subjectCode": "SUBJ",
        "timeUnit": "year",
        "category": "demo",
        "paths": [[{"id": "demo", "label": "Demo"}]],
        "ids": ["Background", "ContentsCode", "Tid"],
        "roles": {"metric": ["ContentsCode"], "time": ["Tid"]},
        "dimensions": {
            "Background": {
                "label": {"en": "background", "sv": "bakgrund"},
                "values": [
                    {"code": "TOT", "label": {"en": "total", "sv": "totalt"}},
                    {"code": "CHILD", "label": {"en": "child", "sv": "barn"}},
                    {"code": "ADULT", "label": {"en": "adult", "sv": "vuxen"}},
                ],
                "extension": {
                    "codeLists": [
                        {"id": "vs_broken", "label": {"en": "Age bands", "sv": "Aldersgrupper"}, "type": "Valueset"}
                    ]
                },
            },
            "ContentsCode": {
                "label": {"en": "measure", "sv": "matt"},
                "values": [{"code": "OBS", "label": {"en": "Observation", "sv": "Observation"}}],
                "extension": {},
            },
            "Tid": {
                "label": {"en": "year", "sv": "ar"},
                "values": [{"code": "2024", "label": {"en": "2024", "sv": "2024"}}],
                "extension": {},
            },
        },
    },
}


CODELISTS = {
    "vs_region_country": {
        "id": "vs_region_country",
        "label": {"en": "country", "sv": "land"},
        "type": "Valueset",
        "values": [{"code": "00", "label": {"en": "Sweden", "sv": "Sverige"}, "valueMap": ["00"]}],
    },
    "vs_region_county": {
        "id": "vs_region_county",
        "label": {"en": "county", "sv": "lan"},
        "type": "Valueset",
        "values": [
            {"code": "01", "label": {"en": "County A", "sv": "Lan A"}, "valueMap": ["01"]},
            {"code": "03", "label": {"en": "County B", "sv": "Lan B"}, "valueMap": ["03"]},
        ],
    },
    "vs_region_country_alias": {
        "id": "vs_region_country_alias",
        "label": {"en": "country alias", "sv": "landsalias"},
        "type": "Valueset",
        "values": [{"code": "SW", "label": {"en": "Sweden alias", "sv": "Sverige alias"}, "valueMap": ["00"]}],
    },
    "agg_region_county_grouped": {
        "id": "agg_region_county_grouped",
        "label": {"en": "county grouped", "sv": "lan grupperad"},
        "type": "Aggregation",
        "values": [
            {"code": "GA", "label": {"en": "County group A", "sv": "Langrupp A"}, "valueMap": ["01"]},
            {"code": "GB", "label": {"en": "County group B", "sv": "Langrupp B"}, "valueMap": ["03"]},
        ],
    },
    "vs_region_untyped": {
        "id": "vs_region_untyped",
        "label": {"en": "county alias", "sv": "lansalias"},
        "type": "Valueset",
        "values": [
            {"code": "XA", "label": {"en": "County A alias", "sv": "Lan A alias"}, "valueMap": ["01"]},
            {"code": "XB", "label": {"en": "County B alias", "sv": "Lan B alias"}, "valueMap": ["03"]},
        ],
    },
}


HIDDEN_CODELISTS = {
    "vs_broken": {
        "id": "vs_broken",
        "label": {"en": "Age bands", "sv": "Aldersgrupper"},
        "type": "Valueset",
        "values": [
            {"code": "TOT", "label": {"en": "total", "sv": "totalt"}, "valueMap": ["TOT"]},
            {"code": "CHILD", "label": {"en": "child", "sv": "barn"}, "valueMap": ["CHILD"]},
            {"code": "ADULT", "label": {"en": "adult", "sv": "vuxen"}, "valueMap": ["ADULT"]},
        ],
    }
}


def localized(value, lang):
    if isinstance(value, dict):
        return value.get(lang, value.get("en", next(iter(value.values()))))
    return value


def table_row(table, lang):
    return {
        "id": table["id"],
        "label": localized(table["label"], lang),
        "description": localized(table["description"], lang),
        "updated": table["updated"],
        "firstPeriod": table["firstPeriod"],
        "lastPeriod": table["lastPeriod"],
        "discontinued": table["discontinued"],
        "source": table["source"],
        "subjectCode": table["subjectCode"],
        "timeUnit": table["timeUnit"],
        "category": table["category"],
        "paths": table["paths"],
    }


def metadata_response(table_id, lang):
    table = TABLES[table_id]
    dimension = {}
    for variable_code in table["ids"]:
        variable = table["dimensions"][variable_code]
        index = {value["code"]: idx for idx, value in enumerate(variable["values"])}
        labels = {value["code"]: localized(value["label"], lang) for value in variable["values"]}
        variable_obj = {
            "label": localized(variable["label"], lang),
            "category": {
                "index": index,
                "label": labels,
            },
            "extension": {},
        }
        if "child" in variable:
            variable_obj["category"]["child"] = variable["child"]
        extension = variable.get("extension", {})
        if extension.get("codeLists"):
            variable_obj["extension"]["codeLists"] = [
                {
                    "id": entry["id"],
                    "label": localized(entry["label"], lang),
                    **({"type": entry["type"]} if "type" in entry else {}),
                }
                for entry in extension["codeLists"]
            ]
        dimension[variable_code] = variable_obj
    return {
        "id": table["ids"],
        "size": [len(table["dimensions"][code]["values"]) for code in table["ids"]],
        "dimension": dimension,
        "role": table["roles"],
        "extension": {"px": {"heading": ["ContentsCode", "Tid"], "stub": [code for code in table["ids"] if code not in {"ContentsCode", "Tid"}]}},
    }


def codelist_response(codelist_id, lang):
    codelist = CODELISTS[codelist_id]
    return {
        "id": codelist["id"],
        "label": localized(codelist["label"], lang),
        "type": codelist["type"],
        "values": [
            {
                "code": value["code"],
                "label": localized(value["label"], lang),
                "valueMap": list(value["valueMap"]),
            }
            for value in codelist["values"]
        ],
    }


def parse_selection(query_params=None, body_text=None):
    if body_text:
        raw = json.loads(body_text)
        return raw.get("selection", [])
    selections = []
    query_params = query_params or {}
    by_var = {}
    for key, values in query_params.items():
        if key.startswith("valueCodes[") or key.startswith("valuecodes["):
            variable = key[key.find("[") + 1 : -1]
            by_var.setdefault(variable, {})["valueCodes"] = values[0].split(",")
        if key.startswith("codelist[") or key.startswith("codeList["):
            variable = key[key.find("[") + 1 : -1]
            by_var.setdefault(variable, {})["codelist"] = values[0]
    for variable, payload in by_var.items():
        selections.append(
            {
                "variableCode": variable,
                "valueCodes": payload.get("valueCodes", ["*"]),
                **({"codelist": payload["codelist"]} if "codelist" in payload else {}),
            }
        )
    return selections


def selected_codes(table, variable_code, selections):
    variable = table["dimensions"][variable_code]
    available = [value["code"] for value in variable["values"]]
    selection = next((entry for entry in selections if entry["variableCode"] == variable_code), None)
    if not selection:
        return available
    requested = selection.get("valueCodes", ["*"])
    if selection.get("codelist"):
        codelist = HIDDEN_CODELISTS.get(selection["codelist"]) or CODELISTS[selection["codelist"]]
        if requested == ["*"]:
            return [value["code"] for value in codelist["values"]]
        return requested
    if requested == ["*"]:
        return available
    if len(requested) == 1 and requested[0].endswith("*"):
        prefix = requested[0][:-1]
        return [code for code in available if code.startswith(prefix)]
    return requested


def label_for_value(table, variable_code, code, lang):
    variable = table["dimensions"][variable_code]
    for value in variable["values"]:
        if value["code"] == code:
            return localized(value["label"], lang)
    for codelist in HIDDEN_CODELISTS.values():
        for value in codelist["values"]:
            if value["code"] == code:
                return localized(value["label"], lang)
    return code


def main_value(coord):
    region = {"00": 0, "01": 1, "03": 3, "SW": 0, "GA": 1, "GB": 3, "XA": 1, "XB": 3}[coord["Region"]]
    sex = {"T": 0, "M": 1, "F": 2}[coord["Sex"]]
    month = {"2024M01": 1, "2024M02": 2}[coord["Tid"]]
    if coord["ContentsCode"] == "POP":
        return float(1000 + region * 100 + sex * 10 + month), None
    if coord["Region"] == "03" and coord["Sex"] == "F" and coord["Tid"] == "2024M02":
        return None, ".."
    return float(10 + region * 100 + sex * 10 + month), None


def tree_value(coord):
    age = {"TOT": 0, "ADULT": 1, "CHILD": 2}[coord["Age"]]
    return float(500 + age * 10), None


def probe_value(coord):
    background = {"TOT": 0, "CHILD": 1, "ADULT": 2}[coord["Background"]]
    return float(900 + background), None


VALUE_FNS = {
    "MOCK_MAIN": main_value,
    "MOCK_TREE": tree_value,
    "MOCK_PROBE": probe_value,
}


def build_jsonstat(table_id, lang, selections):
    table = TABLES[table_id]
    ids = list(table["ids"])
    sizes = []
    dimension = {}
    selected = {}
    for variable_code in ids:
        codes = selected_codes(table, variable_code, selections)
        selected[variable_code] = codes
        sizes.append(len(codes))
        dimension[variable_code] = {
            "label": localized(table["dimensions"][variable_code]["label"], lang),
            "category": {
                "index": {code: idx for idx, code in enumerate(codes)},
                "label": {code: label_for_value(table, variable_code, code, lang) for code in codes},
            },
        }

    values = []
    status = []

    def emit(depth, coord):
        if depth == len(ids):
            value, symbol = VALUE_FNS[table_id](coord)
            values.append(value)
            status.append(symbol)
            return
        variable_code = ids[depth]
        for code in selected[variable_code]:
            coord[variable_code] = code
            emit(depth + 1, coord)

    emit(0, {})
    response = {
        "class": "dataset",
        "id": ids,
        "size": sizes,
        "dimension": dimension,
        "value": values,
    }
    if any(symbol is not None for symbol in status):
        response["status"] = status
    return response


class MockScbHandler(BaseHTTPRequestHandler):
    request_counts = Counter()
    data_requests = []

    def log_message(self, fmt, *args):
        return

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _record(self, key):
        MockScbHandler.request_counts[key] += 1

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        path = parsed.path
        lang = query.get("lang", ["en"])[0]
        if path == "/api/v2/config":
            self._record("GET /config")
            self._send_json(
                {
                    "apiVersion": "2.0",
                    "appVersion": "mock",
                    "defaultLanguage": "en",
                    "languages": ["en", "sv"],
                    "maxDataCells": 4,
                    "maxCallsPerTimeWindow": 30,
                    "timeWindow": 10,
                    "defaultDataFormat": "json-stat2",
                    "dataFormats": ["json-stat2", "csv"],
                }
            )
            return
        if path == "/api/v2/tables":
            self._record("GET /tables")
            include_discontinued = query.get("includeDiscontinued", ["false"])[0].lower() == "true"
            needle = query.get("query", [""])[0].lower()
            rows = []
            for table in TABLES.values():
                if not include_discontinued and table["discontinued"]:
                    continue
                haystack = f"{table['id']} {localized(table['label'], lang)}".lower()
                if needle and needle not in haystack:
                    continue
                rows.append(table_row(table, lang))
            rows.sort(key=lambda entry: entry["id"])
            page_number = int(query.get("pageNumber", ["1"])[0])
            page_size = int(query.get("pageSize", ["1000"])[0])
            start = (page_number - 1) * page_size
            end = start + page_size
            total_pages = max(1, (len(rows) + page_size - 1) // page_size)
            self._send_json({"tables": rows[start:end], "page": {"totalPages": total_pages}})
            return
        if path.startswith("/api/v2/tables/") and path.endswith("/metadata"):
            table_id = unquote(path.split("/")[4])
            self._record(f"GET /tables/{table_id}/metadata")
            self._send_json(metadata_response(table_id, lang))
            return
        if path.startswith("/api/v2/codelists/"):
            codelist_id = unquote(path.split("/")[4])
            self._record(f"GET /codelists/{codelist_id}")
            if codelist_id == "vs_broken":
                self._send_json({"error": "not found"}, status=404)
                return
            self._send_json(codelist_response(codelist_id, lang))
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query, keep_blank_values=True)
        lang = query.get("lang", ["en"])[0]
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        if path.startswith("/api/v2/tables/") and path.endswith("/data"):
            table_id = unquote(path.split("/")[4])
            self._record(f"POST /tables/{table_id}/data")
            selections = parse_selection(body_text=body)
            MockScbHandler.data_requests.append({"table_id": table_id, "selection": selections})
            self._send_json(build_jsonstat(table_id, lang, selections))
            return
        self._send_json({"error": "not found"}, status=404)


def start_server():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = ThreadingHTTPServer(("127.0.0.1", port), MockScbHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, port


def run_query(sql, base_url, expect_success=True):
    env = os.environ.copy()
    env["SCB_API_BASE_URL"] = base_url
    proc = subprocess.run(
        [str(DUCKDB_BIN), "-unsigned", "-json", "-c", sql],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if expect_success and proc.returncode != 0:
        raise AssertionError(f"query failed:\n{sql}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    if not expect_success:
        if proc.returncode == 0:
            raise AssertionError(f"query unexpectedly succeeded:\n{sql}\nstdout:\n{proc.stdout}")
        return proc
    stdout = proc.stdout.strip()
    if stdout == "[{]":
        return []
    return json.loads(stdout) if stdout else []


def assert_equal(actual, expected, message):
    if actual != expected:
        raise AssertionError(f"{message}\nexpected: {expected}\nactual:   {actual}")


def main():
    server, port = start_server()
    base_url = f"http://127.0.0.1:{port}/api/v2"
    try:
        functions = run_query(
            """
            SELECT DISTINCT function_name
            FROM duckdb_functions()
            WHERE function_name IN ('scb_config', 'scb_scan', 'scb_table', 'scb_tables', 'scb_values', 'scb_variables')
            ORDER BY function_name
            """,
            base_url,
        )
        assert_equal(
            [row["function_name"] for row in functions],
            ["scb_config", "scb_scan", "scb_table", "scb_tables", "scb_values", "scb_variables"],
            "public functions changed",
        )

        config = run_query(
            "SELECT api_version, default_language, max_data_cells, max_calls_per_time_window, time_window FROM scb_config()",
            base_url,
        )
        assert_equal(
            config,
            [{"api_version": "2.0", "default_language": "en", "max_data_cells": 4, "max_calls_per_time_window": 30, "time_window": 10}],
            "scb_config result changed",
        )

        tables_default = run_query(
            "SELECT table_id FROM scb_tables(lang := 'en') ORDER BY table_id",
            base_url,
        )
        assert_equal(
            [row["table_id"] for row in tables_default],
            ["MOCK_MAIN", "MOCK_TREE"],
            "scb_tables should exclude discontinued tables by default",
        )

        tables_filtered = run_query(
            "SELECT table_id, language, CASE WHEN discontinued THEN 1 ELSE 0 END AS discontinued FROM scb_tables(lang := 'sv', query := 'mock', include_discontinued := true, past_days := 14) ORDER BY table_id",
            base_url,
        )
        assert_equal(
            tables_filtered,
            [
                {"table_id": "MOCK_MAIN", "language": "sv", "discontinued": 0},
                {"table_id": "MOCK_PROBE", "language": "sv", "discontinued": 1},
                {"table_id": "MOCK_TREE", "language": "sv", "discontinued": 0},
            ],
            "scb_tables filtering changed",
        )

        variables = run_query(
            "SELECT variable_code, role, variant, variant_name, variant_kind, member_count FROM scb_variables('MOCK_MAIN', lang := 'en') ORDER BY variable_code, variant",
            base_url,
        )
        assert_equal(
            variables,
            [
                {"variable_code": "Region", "role": "geo", "variant": 0, "variant_name": "country", "variant_kind": "valueset", "member_count": 1},
                {"variable_code": "Region", "role": "geo", "variant": 1, "variant_name": "country alias", "variant_kind": "valueset", "member_count": 1},
                {"variable_code": "Region", "role": "geo", "variant": 2, "variant_name": "county", "variant_kind": "valueset", "member_count": 2},
                {"variable_code": "Region", "role": "geo", "variant": 3, "variant_name": "county alias", "variant_kind": "valueset", "member_count": 2},
                {"variable_code": "Region", "role": "geo", "variant": 4, "variant_name": "county grouped", "variant_kind": "aggregation", "member_count": 2},
                {"variable_code": "Sex", "role": "", "variant": 0, "variant_name": "all", "variant_kind": "flat", "member_count": 3},
                {"variable_code": "Tid", "role": "time", "variant": 0, "variant_name": "all", "variant_kind": "flat", "member_count": 2},
            ],
            "scb_variables result changed",
        )

        values = run_query(
            "SELECT value_code, label, sort_index, parent_code, child_count, CASE WHEN is_leaf THEN 1 ELSE 0 END AS is_leaf FROM scb_values('MOCK_TREE', 'Age', lang := 'en') ORDER BY sort_index",
            base_url,
        )
        assert_equal(
            values,
            [
                {"value_code": "TOT", "label": "total", "sort_index": 0, "parent_code": "", "child_count": 2, "is_leaf": 0},
                {"value_code": "ADULT", "label": "adult", "sort_index": 1, "parent_code": "TOT", "child_count": 0, "is_leaf": 1},
                {"value_code": "CHILD", "label": "child", "sort_index": 2, "parent_code": "TOT", "child_count": 0, "is_leaf": 1},
            ],
            "scb_values result changed",
        )

        variables_tree = run_query(
            "SELECT variable_code, role, variant, variant_name, variant_kind, member_count FROM scb_variables('MOCK_TREE', lang := 'en') ORDER BY variable_code, variant",
            base_url,
        )
        assert_equal(
            variables_tree,
            [
                {"variable_code": "Age", "role": "", "variant": 0, "variant_name": "depth 0", "variant_kind": "hierarchy", "member_count": 1},
                {"variable_code": "Age", "role": "", "variant": 1, "variant_name": "depth 1", "variant_kind": "hierarchy", "member_count": 2},
                {"variable_code": "Tid", "role": "time", "variant": 0, "variant_name": "all", "variant_kind": "flat", "member_count": 1},
            ],
            "scb_variables hierarchy result changed",
        )

        variables_probe = run_query(
            "SELECT variable_code, role, variant, variant_name, variant_kind, member_count FROM scb_variables('MOCK_PROBE', lang := 'en') ORDER BY variable_code, variant",
            base_url,
        )
        assert_equal(
            variables_probe,
            [
                {"variable_code": "Background", "role": "", "variant": 0, "variant_name": "Age bands", "variant_kind": "valueset", "member_count": 3},
                {"variable_code": "Tid", "role": "time", "variant": 0, "variant_name": "all", "variant_kind": "flat", "member_count": 1},
            ],
            "scb_variables probe fallback changed",
        )

        describe = run_query("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM scb_scan('MOCK_MAIN'))", base_url)
        assert_equal(
            [(row["column_name"], row["column_type"]) for row in describe],
            [
                ("region", "VARCHAR"),
                ("sex", "VARCHAR"),
                ("month", "VARCHAR"),
                ("Population", "DOUBLE"),
                ("Population change", "DOUBLE"),
            ],
            "scb_scan schema changed",
        )

        scan_default = run_query(
            "SELECT region, sex, month, Population, \"Population change\" FROM scb_scan('MOCK_MAIN') ORDER BY sex, month",
            base_url,
        )
        assert_equal(
            scan_default,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0, "Population change": 21.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0, "Population change": 22.0},
                {"region": "Sweden", "sex": "total", "month": "2024M01", "Population": 1001.0, "Population change": 11.0},
                {"region": "Sweden", "sex": "total", "month": "2024M02", "Population": 1002.0, "Population change": 12.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0, "Population change": 31.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0, "Population change": 32.0},
            ],
            "default scb_scan result changed",
        )

        scan_alias = run_query(
            "SELECT region, sex, month, Population, \"Population change\" FROM scb_table('MOCK_MAIN') ORDER BY sex, month",
            base_url,
        )
        assert_equal(scan_alias, scan_default, "scb_table alias result changed")

        scan_named_variant = run_query(
            """
            SELECT region, sex, month, Population, "Population change"
            FROM scb_scan('MOCK_MAIN', {'region': 'COUNTY'})
            WHERE region = 'County B' AND sex = 'women' AND month = '2024M02'
            """,
            base_url,
        )
        assert_equal(
            scan_named_variant,
            [{"region": "County B", "sex": "women", "month": "2024M02", "Population": 1322.0, "Population change": None}],
            "named scb_scan variant resolution or status handling changed",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        scan_codelist_variant = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN', {'Region': 'country alias'})
            WHERE region = 'Sweden alias' AND sex = 'men' AND month = '2024M01'
            """,
            base_url,
        )
        assert_equal(
            scan_codelist_variant,
            [{"region": "Sweden alias", "sex": "men", "month": "2024M01", "Population": 1011.0}],
            "codelist-backed variant scan changed",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["SW"], "codelist": "vs_region_country_alias"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01"]},
                    ],
                }
            ],
            "codelist-backed variants should preserve the SCB codelist selection",
        )

        scan_untyped_variant = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN', {'Region': 'county alias'})
            WHERE region = 'County B alias' AND sex = 'men' AND month = '2024M01'
            """,
            base_url,
        )
        assert_equal(
            scan_untyped_variant,
            [{"region": "County B alias", "sex": "men", "month": "2024M01", "Population": 1311.0}],
            "metadata codelists without an explicit type should still work as variants",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        pushed_equals = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'men'
            ORDER BY month
            """,
            base_url,
        )
        assert_equal(
            pushed_equals,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
            ],
            "equality filter pushdown result changed",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            1,
            "equality filter pushdown should narrow the SCB request to one chunk",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                }
            ],
            "equality filter pushdown should send narrowed valueCodes",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        pushed_in = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex IN ('men', 'women') AND month = '2024M01'
            ORDER BY sex
            """,
            base_url,
        )
        assert_equal(
            pushed_in,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0},
            ],
            "IN filter pushdown result changed",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            1,
            "IN filter pushdown should narrow the SCB request to one chunk",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M", "F"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01"]},
                    ],
                }
            ],
            "IN filter pushdown should send narrowed valueCodes",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        pushed_or = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'men' OR sex = 'women'
            ORDER BY sex, month
            """,
            base_url,
        )
        assert_equal(
            pushed_or,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0},
            ],
            "OR filter pushdown result changed",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            2,
            "OR filter pushdown should narrow the SCB request before chunking",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["F"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
            ],
            "OR filter pushdown should send narrowed valueCodes",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        pushed_range = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE month >= '2024M02' AND month <= '2024M02'
            ORDER BY sex
            """,
            base_url,
        )
        assert_equal(
            pushed_range,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
                {"region": "Sweden", "sex": "total", "month": "2024M02", "Population": 1002.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0},
            ],
            "range-style filter pushdown result changed",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            2,
            "range-style filter pushdown should reduce the selected month before chunking",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["T"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M", "F"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M02"]},
                    ],
                },
            ],
            "range-style filter pushdown should narrow month valueCodes before chunking",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        pushed_hidden_column = run_query(
            """
            SELECT month
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'men'
            ORDER BY month
            """,
            base_url,
        )
        assert_equal(
            pushed_hidden_column,
            [
                {"month": "2024M01"},
                {"month": "2024M02"},
            ],
            "filter on non-projected dimension should still push down correctly",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            1,
            "non-projected dimension filter should still narrow the SCB request",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                }
            ],
            "non-projected dimension filter should translate to the right valueCodes",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        no_match = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'missing'
            """,
            base_url,
        )
        assert_equal(no_match, [], "zero-match pushed filter should return no rows")
        assert_equal(
            MockScbHandler.request_counts.get("POST /tables/MOCK_MAIN/data", 0),
            0,
            "zero-match pushed filter should avoid issuing a data request",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        null_match = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex IS NULL
            """,
            base_url,
        )
        assert_equal(null_match, [], "IS NULL on a dimension should return no rows")
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            3,
            "IS NULL on a dimension is currently a residual DuckDB filter and should not narrow the SCB selection",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        not_null_match = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex IS NOT NULL
            ORDER BY sex, month
            """,
            base_url,
        )
        assert_equal(
            not_null_match,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
                {"region": "Sweden", "sex": "total", "month": "2024M01", "Population": 1001.0},
                {"region": "Sweden", "sex": "total", "month": "2024M02", "Population": 1002.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0},
            ],
            "IS NOT NULL on a dimension should preserve all rows",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            3,
            "IS NOT NULL on a dimension should not narrow the pushed SCB selection",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        mixed_filtered = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'men' AND Population > 1011
            ORDER BY month
            """,
            base_url,
        )
        assert_equal(
            mixed_filtered,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
            ],
            "mixed pushed/residual filters changed",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            1,
            "mixed pushed/residual filters should keep the pushed dimension narrowing",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                }
            ],
            "mixed pushed/residual filters should not leak metric filtering into the SCB selection",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        cross_column_or = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE sex = 'men' OR Population > 1011
            ORDER BY sex, month
            """,
            base_url,
        )
        assert_equal(
            cross_column_or,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0},
            ],
            "cross-column OR should remain a residual DuckDB filter",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            3,
            "cross-column OR should not partially narrow the pushed SCB selection",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["T"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["F"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
            ],
            "cross-column OR should not leak any partial pushdown into the SCB selection",
        )

        MockScbHandler.request_counts.clear()
        MockScbHandler.data_requests.clear()
        metric_filtered = run_query(
            """
            SELECT region, sex, month, Population
            FROM scb_scan('MOCK_MAIN')
            WHERE Population > 1010
            ORDER BY sex, month
            """,
            base_url,
        )
        assert_equal(
            metric_filtered,
            [
                {"region": "Sweden", "sex": "men", "month": "2024M01", "Population": 1011.0},
                {"region": "Sweden", "sex": "men", "month": "2024M02", "Population": 1012.0},
                {"region": "Sweden", "sex": "women", "month": "2024M01", "Population": 1021.0},
                {"region": "Sweden", "sex": "women", "month": "2024M02", "Population": 1022.0},
            ],
            "metric filter should remain a residual DuckDB filter",
        )
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            3,
            "metric filter should not change the pushed SCB selection",
        )
        assert_equal(
            MockScbHandler.data_requests,
            [
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["T"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["M"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
                {
                    "table_id": "MOCK_MAIN",
                    "selection": [
                        {"variableCode": "Region", "valueCodes": ["00"], "codelist": "vs_region_country"},
                        {"variableCode": "Sex", "valueCodes": ["F"]},
                        {"variableCode": "ContentsCode", "valueCodes": ["POP", "INC"]},
                        {"variableCode": "Tid", "valueCodes": ["2024M01", "2024M02"]},
                    ],
                },
            ],
            "metric filter should not leak into the pushed SCB selection",
        )

        scan_tree = run_query(
            "SELECT age, year, Observation FROM scb_scan('MOCK_TREE', {'Age': 1}, 'en') ORDER BY age",
            base_url,
        )
        assert_equal(
            scan_tree,
            [
                {"age": "adult", "year": "2024", "Observation": 510.0},
                {"age": "child", "year": "2024", "Observation": 520.0},
            ],
            "hierarchy scan changed",
        )

        failure = run_query("SELECT * FROM scb_scan('MOCK_MAIN', {'Nope': 0})", base_url, expect_success=False)
        if "Unknown SCB variant dimension" not in failure.stderr:
            raise AssertionError(f"unexpected error for invalid variant dimension:\n{failure.stderr}")

        MockScbHandler.request_counts.clear()
        cached_counts = run_query(
            """
            SELECT
                (SELECT COUNT(*) FROM scb_scan('MOCK_MAIN')) AS first_count,
                (SELECT COUNT(*) FROM scb_scan('MOCK_MAIN')) AS second_count
            """,
            base_url,
        )
        assert_equal(cached_counts, [{"first_count": 6, "second_count": 6}], "cached scan counts changed")
        assert_equal(
            MockScbHandler.request_counts["POST /tables/MOCK_MAIN/data"],
            3,
            "repeated identical scan should reuse cached scan chunks within one process",
        )

        print("mock SCB API tests passed")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
