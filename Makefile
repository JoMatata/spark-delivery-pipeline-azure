PY ?= python
ROWS ?= 50000
export PYTHONPATH := src

.PHONY: help install data pipeline bronze silver gold dq report test benchmark wheel dag-check all clean

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' Makefile | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

install:         ## install Python dependencies
	$(PY) -m pip install -r requirements.txt

data:            ## generate synthetic landing data (ROWS=50000 by default)
	$(PY) src/generate_data.py --rows $(ROWS) --out data/landing

pipeline:        ## run bronze -> silver -> gold -> dq locally
	$(PY) -m pipeline.run --stage all --profile local

bronze silver gold dq:
	$(PY) -m pipeline.run --stage $@ --profile local

report:          ## build charts + insights from the gold tables
	$(PY) scripts/make_report.py

test:            ## run the pytest suite (local SparkSession)
	$(PY) -m pytest

benchmark:       ## 5M-order end-to-end benchmark -> docs/benchmark.md
	$(PY) scripts/benchmark.py --rows 5000000

wheel:           ## build the wheel deployed to Azure Databricks
	$(PY) -m pip wheel . --no-deps -w dist/

dag-check:       ## syntax-check the Airflow DAG
	$(PY) -m py_compile dags/delivery_pipeline_dag.py

all: data pipeline report  ## generate data, run pipeline, build report

clean:           ## remove generated data
	rm -rf data/ spark-warehouse/ dist/ build/
