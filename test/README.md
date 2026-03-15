# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. Build first, then run the tests:
```bash
make -j8
make test
```
or 
```bash
make test_debug
```

The normal validation sequence for this repo is:

- `make -j8`
- `make test`

`make test` also runs `test/scripts/run_mock_api_tests.py`, which starts a local mock PxWeb server and exercises the public `scb_*` SQL API end to end without relying on the live SCB service.
