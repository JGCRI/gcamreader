# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.5.0] - 2026-06-15

This release modernizes the package without changing its computational results.
A behavioral baseline captured from v1.4.0 is preserved in `benchmarks/` and is
used as a regression gate to guarantee identical query outputs.

### Added

- Behavioral baseline harness and regression test suite in `benchmarks/` that
  lock query, scenario, `importdata`, and CLI outputs to the v1.4.0 results.
- `pyproject.toml` as the single source of build metadata and configuration.
- `CHANGELOG.md` to track project changes.

### Changed

- Migrated packaging from `setup.py` and `setup.cfg` to `pyproject.toml`
  (setuptools backend).
- Raised the minimum supported Python version to 3.11.
- Replaced the Click based command line interface (and the
  `click-default-group-wheel` dependency) with a Typer based CLI.
- Relaxed the pinned `requests~=2.20.0` requirement to a modern minimum and
  refreshed the `pandas` and `lxml` minimum versions.
- Modernized `querymi.py`: removed Python 2 and Python 3.5 compatibility
  branches, replaced `pkg_resources` with `importlib.resources`, and added type
  hints and complete docstrings.
- Moved the test suite to a top level `tests/` directory and the example
  notebooks to a top level `notebooks/` directory.

### Removed

- `setup.py`, `setup.cfg`, `requirements.txt`, and `MANIFEST.in` (superseded by
  `pyproject.toml`).

### Release verification

**Result: PASS — v1.5.0 reproduces the v1.4.0 outputs exactly.**

A behavioral baseline captured from v1.4.0 (`benchmarks/baseline/`) is re-run
against the v1.5.0 code by `benchmarks/test_regression.py`. Every fixture
matches: CSV outputs are compared byte-for-byte by SHA-256 checksum, and
DataFrame outputs are compared with `pandas.testing.assert_frame_equal` after a
deterministic sort.

| Operation | Result | Comparison | Java |
| --- | --- | --- | --- |
| `parse_batch_query` (query structure) | identical | exact title/region/query strings | not required |
| `listScenariosInDB` | identical | `assert_frame_equal` | required |
| `runQuery` (land-allocation query) | identical | `assert_frame_equal` (sorted) | required |
| `importdata` (land-allocation query) | identical | `assert_frame_equal` (sorted) | required |
| `gcamreader local` CLI CSV output | identical | SHA-256 (byte-for-byte) | required |

Baseline captured with gcamreader 1.4.0 on Python 3.13.3, OpenJDK 23.0.2
(Homebrew), macOS 26.5.1 (arm64). Reproduce locally with:

```bash
pytest benchmarks/test_regression.py
```

[Unreleased]: https://github.com/JGCRI/gcamreader/compare/v1.5.0...HEAD
[1.5.0]: https://github.com/JGCRI/gcamreader/compare/v1.4.0...v1.5.0
