# Behavioral Baseline (Phase 0)

This directory captures the exact behavior of the **current** `gcamreader`
package (v1.4.0) so that the v1.5.0 modernization can be proven to produce
identical results.

## Contents

- `generate_baseline.py` - regenerates the reference fixtures from the
  installed `gcamreader` package and the bundled sample BaseX database.
- `baseline/` - the committed reference fixtures:
  - `manifest.json` - package version, Python version, Java version, and a
    SHA-256 checksum for every fixture.
  - `parse_batch_query.json` - parsed query structure (titles, regions, query
    strings).
  - `query_outputs/` - CSV outputs for `listScenariosInDB`, `runQuery`
    (land-allocation query), and `importdata`.
  - `cli_outputs/` - CSV files produced by the `gcamreader local` CLI command.
- `test_regression.py` - pytest module that re-runs the same operations and
  asserts equality against the committed fixtures.

## Usage

Regenerate the baseline (only needed if intentionally re-establishing it):

```bash
python benchmarks/generate_baseline.py
```

Run the regression gate:

```bash
pytest benchmarks/test_regression.py
```

The regression tests require a Java runtime and the bundled sample database.
The structural `parse_batch_query` test runs without Java; the database-backed
tests are skipped when Java is unavailable.

This suite is run after each phase of the v1.5.0 work and must continue to pass
unchanged.
