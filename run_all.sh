#!/usr/bin/env bash
# One-shot local run: synthetic data -> medallion pipeline -> charts.
#   ./run_all.sh            # 50k orders (fast)
#   ROWS=5000000 ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"
ROWS="${ROWS:-50000}"
export PYTHONPATH=src

echo "==> Generating ${ROWS} synthetic orders"
python src/generate_data.py --rows "${ROWS}" --out data/landing

echo "==> Running bronze -> silver -> gold -> dq"
python -m pipeline.run --stage all --profile local

echo "==> Building charts from gold tables"
python scripts/make_report.py

echo "Done. Lake in data/lake, DQ report in data/reports/dq_report.json, charts in docs/images/"
