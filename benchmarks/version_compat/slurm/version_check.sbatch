#!/bin/bash
#SBATCH --job-name=gcamreader
#SBATCH --nodes=1
#SBATCH -p short
#SBATCH --time=00:30:00
#SBATCH -A gcims
#SBATCH --cpus-per-task=2
#SBATCH --mem=20G
#SBATCH --array=0-21
#SBATCH --output=gcamreader__%A_%a.out
#SBATCH --error=gcamreader__%A_%a.err

# Load necessary modules
module purge
module load python/3.13.5
module load java/17.0.18

# Activate the virtual environment that contains gcamreader
source /people/d3y010/envs/gcamreader/bin/activate

# --- configuration -------------------------------------------------------
DB_ROOT=/rcfs/projects/GCAM/gcam-ci-run
BASEX_DIR=database_basexdbGCAM
HARNESS_DIR=$HOME/repos/github/gcamreader/benchmarks/version_compat
QUERY=$HARNESS_DIR/queries/land_allocation.xml
OUT=$HARNESS_DIR/results/per_version
MAX_MEM=16g

# Discrete version labels (order defines the array index mapping).
# IMPORTANT: keep --array=0-N in sync with the length of this list (N = len-1).
VERSIONS=(
  gcam-v5.3 gcam-v5.4 gcam-v6.0
  gcam-v7.0 gcam-v7.1 gcam-v7.2 gcam-v7.3 gcam-v7.4
  gcam-v8.0 gcam-v8.1 gcam-v8.2 gcam-v8.3 gcam-v8.4 gcam-v8.5
  gcam-v8.6 gcam-v8.7 gcam-v8.8 gcam-v8.9 gcam-v8.10 gcam-v8.11
  gcam-v9.0 gcam-v9.1
)

VERSION=${VERSIONS[$SLURM_ARRAY_TASK_ID]}
echo "Task ${SLURM_ARRAY_TASK_ID} -> ${VERSION}"

# Confirm java resolves before running (fast-fail with a clear message).
java -version || { echo "java not on PATH"; exit 1; }

python "$HARNESS_DIR/run_version_check.py" \
  --version    "$VERSION" \
  --db-root    "$DB_ROOT" \
  --basex-dir  "$BASEX_DIR" \
  --query      "$QUERY" \
  --out        "$OUT" \
  --max-memory "$MAX_MEM"
