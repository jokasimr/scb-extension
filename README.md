# SCB DuckDB Extension

DuckDB extension for querying Statistics Sweden's PxWeb API as labeled tables instead of raw API payloads.

The exposed interface is:

- `scb_config()`: live API configuration such as languages, formats, and rate limits.
- `scb_tables(...)`: discover available SCB tables.
- `scb_variables(table_id, lang := 'en')`: per-variable variants for one table, including the SCB role.
- `scb_values(table_id, variable_code, lang := 'en')`: value metadata for one variable.
- `scb_scan(table_id, variants := NULL, lang := 'en')`: scan one SCB table at the requested variant per dimension.
- `scb_table(table_id, variants := NULL, lang := 'en')`: alias for `scb_scan`.

`scb_scan` is the main user-facing entry point. Instead of passing raw SCB selections, you pass a variant map such as:

```sql
SELECT *
FROM scb_scan('TAB6473', {'Region': 1, 'Kon': 0}, 'en');
```

That means:

- `Region = 1`: the second published variant for `Region` in `scb_variables`
- `Kon = 0`: the first published variant for `Kon`
- unspecified dimensions use variant `0`

Variants map directly to explicit SCB structures where available:

- `valueset` variants come from SCB codelists/valuesets
- `hierarchy` variants come from explicit parent/child metadata depths
- `flat` means SCB exposed no alternate structure for that variable

The result is a regular-looking table:

- coordinate columns contain labels, not raw SCB codes
- time dimensions are exposed as regular dimension columns
- all metrics are returned as separate value columns
- cells with SCB status symbols are exposed as `NULL`
- SCB status/symbol columns are not included in the scan output

Metadata and `json-stat2` scan payloads are cached through DuckDB's built-in object cache.

## Building

Build the extension with:

```sh
make
```

The main artifacts are:

```sh
./build/release/duckdb
./build/release/unittest
./build/release/extension/scb/scb.duckdb_extension
```

`./build/release/duckdb` already has the static extension linked in for local development.

## Examples

List the SCB functions:

```sql
SELECT function_name, function_type
FROM duckdb_functions()
WHERE function_name LIKE 'scb_%'
ORDER BY function_name;
```

Inspect metadata:

```sql
SELECT * FROM scb_config();
SELECT * FROM scb_tables(lang := 'en') LIMIT 10;
SELECT * FROM scb_variables('TAB6473', lang := 'en');
SELECT * FROM scb_values('TAB6473', 'Region', lang := 'en');
```

Scan a table at a chosen variant:

```sql
SELECT *
FROM scb_scan('TAB6473', {'Region': 1, 'Kon': 0}, 'en')
LIMIT 10;
```

`variants` may be:

- `NULL`
- a JSON object string, e.g. `'{"Region":1,"Kon":0}'`
- a `MAP(VARCHAR, BIGINT)`
- a `STRUCT`, e.g. `{'Region': 1, 'Kon': 0}`

## Current Notes

- Variant derivation uses only explicit SCB structures: valueset codelists and metadata hierarchies.
- Variables without explicit SCB variant structure are exposed as a single `flat` variant named `all`.
- Time dimensions are returned as ordinary labeled dimension columns.
- The scan path uses SCB's live `json-stat2` format and materializes rows directly in the extension.

## Tests

Build first, then run the tests:

```sh
make -j8
make test
```

Do not assume `make test` will rebuild the binary you intend to validate. The expected validation sequence in this
repo is always:

- `make -j8`
- `make test`

`make test` now runs both:

- the lightweight SQLLogic surface test
- a deterministic mock-SCB end-to-end harness that exercises all public functions, scan chunking, status-to-`NULL` handling, explicit hierarchies, valueset variants, broken-codelist fallback, and in-process scan caching
