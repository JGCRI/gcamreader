# GCAM Version Compatibility & Performance

This directory verifies that `gcamreader` works against GCAM output databases
produced by every available GCAM version (5.3 through 9.1), and captures
per-version performance figures. It is designed to run on an HPC cluster under
Slurm, after which its outputs are committed here as documentation.

The full design rationale lives in
[`plans/gcam-version-verification.md`](../../plans/gcam-version-verification.md).

## Contents

- `run_version_check.py` — per-version harness. Runs exactly one GCAM version
  (selected with `--version`) so it maps cleanly onto a Slurm array task. Opens
  a `LocalDBConn`, lists scenarios, and runs a benchmark query, timing each
  stage and writing a single JSON record. Database failures are captured as
  status codes rather than raised, so one bad version cannot abort the array.
- `aggregate_results.py` — merges all per-version JSON records into
  `results/version_compat_summary.csv` and `results/VERIFIED_VERSIONS.md`.
- `plot_query_rows.py` — renders a self-contained SVG diagnostic of the
  benchmark query's row count across versions (no matplotlib required) into
  `docs/_static/query_rows_by_version.svg`.
- `queries/land_allocation.xml` — the cross-version benchmark query (a copy of
  the bundled land-allocation query). Add more XML files here to broaden
  coverage.
- `slurm/version_check.sbatch` — Slurm array job that runs all 22 versions as
  independent tasks.
- `results/` — populated on the cluster, then committed:
  - `per_version/<version>.json` — one record per version.
  - `version_compat_summary.csv` — flattened summary table.
  - `VERIFIED_VERSIONS.md` — status matrix and timing report.

## Confirmed data layout

Each version's database lives at:

```
/rcfs/projects/GCAM/gcam-ci-run/<version>/output/database_basexdbGCAM/
    atv.basex  inf.basex  tbl.basex  tbli.basex  txt.basex  txtl.basex  txtr.basex
```

## Usage on an HPC cluster

```bash
# 1. Sync this repo to the cluster (or git pull there).
cd $HOME/repos/github/gcamreader

# 2. Sanity-check a single version interactively first.
module purge && module load python/3.13.5 java/17.0.18
source $HOME/envs/gcamreader/bin/activate
python benchmarks/version_compat/run_version_check.py \
  --version gcam-v8.2 \
  --db-root /rcfs/projects/GCAM/gcam-ci-run \
  --query benchmarks/version_compat/queries/land_allocation.xml \
  --out /tmp/vc_test

# 3. Launch the full array (22 tasks).
sbatch benchmarks/version_compat/slurm/version_check.sbatch

# 4. After completion, aggregate.
python benchmarks/version_compat/aggregate_results.py \
  --in  benchmarks/version_compat/results/per_version \
  --out benchmarks/version_compat/results
```

The harness and Slurm script require a Java runtime (`java/17.0.18`) and access
to the GCAM database filesystem. Both are typically provided on an HPC cluster
via Lmod.

## Status code reference

| Status | Meaning |
|--------|---------|
| `PASS` | Connect + scenario list + benchmark query all succeeded with data |
| `MISSING` | No `*.basex` files found under the expected directory |
| `CONNECT_FAIL` | `LocalDBConn` construction raised |
| `SCENARIO_FAIL` | `listScenariosInDB` raised or returned `None` |
| `QUERY_FAIL` | `runQuery` raised (e.g., schema/XQuery incompatibility) |
| `QUERY_EMPTY` | Query ran but returned no rows (possible schema drift) |

`QUERY_EMPTY` and `QUERY_FAIL` are the most informative signals for detecting
where the bundled queries diverge from a given GCAM schema.

## Latest results

The committed run (`gcamreader 1.5.0`, Python 3.13.5, Java 17.0.18 on an HPC
cluster) verified **all 22 of 22** versions as `PASS`. See
[`results/VERIFIED_VERSIONS.md`](results/VERIFIED_VERSIONS.md) for the full
matrix and [`docs/verified_versions.rst`](../../docs/verified_versions.rst) for
the rendered documentation page.

- **GCAM 5.3 - 9.1**: all `PASS`. The land-allocation query returns 477k-747k
  rows in roughly 33-60 seconds at `-Xmx16g`; scenario listing takes under ~2
  seconds.
- **Row counts are non-monotonic.** gcam-v6.0 returns the most rows (747k),
  after which the 7.x land-module redesign resets the count to 653k; from there
  it grows gradually through 9.1 (683k). This tracks GCAM's evolving land
  representation, not `gcamreader` behavior - see the diagnostic figure and
  tier table in the rendered docs page.

> An earlier run reported `SCENARIO_FAIL` for gcam-v5.3/v5.4/v6.0 due to a
> filesystem permission error (`tbl.basex (Permission denied)`) on the CI-owned
> databases. After those databases were made readable, all three PASS.
