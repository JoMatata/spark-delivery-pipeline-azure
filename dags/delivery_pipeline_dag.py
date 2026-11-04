"""Airflow DAG: daily medallion run  bronze >> silver >> gold >> dq_checks.

Two execution modes, chosen with the env var DELIVERY_PIPELINE_MODE (read at parse time from
the environment, not from the metadata DB):

* "local"      - python/Spark on the Airflow worker (dev, CI, small volumes)
* "databricks" - each stage is submitted to Azure Databricks as a one-off run on a job
                 cluster (DatabricksSubmitRunOperator, apache-airflow-providers-databricks),
                 executing the same wheel entry point as azure/databricks_job.json

Compatible with Airflow 2.7+ and 3.x.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3 moved the core operators into the standard provider
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("DELIVERY_PIPELINE_DIR", "/opt/airflow/repos/spark-delivery-pipeline-azure")
MODE = os.environ.get("DELIVERY_PIPELINE_MODE", "local")
PROFILE = os.environ.get("DELIVERY_PIPELINE_PROFILE", "local")
WHEEL = "/Volumes/delivery/artifacts/wheels/nairobi_delivery_pipeline-1.0.0-py3-none-any.whl"
JOB_CLUSTER = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_D4ds_v5",
    "autoscale": {"min_workers": 2, "max_workers": 6},
    "data_security_mode": "SINGLE_USER",
    "azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE", "first_on_demand": 1},
    "spark_env_vars": {"PIPELINE_PROFILE": "azure", "AZURE_STORAGE_ACCOUNT": "stnbodeliverylake"},
}
STAGES = ["bronze", "silver", "gold", "dq"]

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="nairobi_delivery_medallion",
    description="Nairobi delivery orders: landing -> bronze -> silver -> gold -> DQ gate",
    start_date=datetime(2025, 10, 1),
    schedule="30 2 * * *",           # 02:30 EAT, after the nightly OMS export lands in ADLS
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["delivery", "pyspark", "medallion", "azure"],
    doc_md=__doc__,
) as dag:
    if MODE == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        tasks = {
            stage: DatabricksSubmitRunOperator(
                task_id=stage if stage != "dq" else "dq_checks",
                databricks_conn_id="databricks_default",
                run_name=f"nairobi-delivery-{stage}",
                new_cluster=JOB_CLUSTER,
                libraries=[{"whl": WHEEL}],
                python_wheel_task={
                    "package_name": "nairobi_delivery_pipeline",
                    "entry_point": "delivery-pipeline",
                    "parameters": ["--stage", stage, "--profile", "azure",
                                   "--config", "/Volumes/delivery/artifacts/conf/config.yaml"],
                },
                wait_for_termination=True,
            )
            for stage in STAGES
        }
    else:
        tasks = {
            stage: BashOperator(
                task_id=stage if stage != "dq" else "dq_checks",
                bash_command=(f"cd {PROJECT_DIR} && PYTHONPATH=src "
                              f"python -m pipeline.run --stage {stage} --profile {PROFILE}"),
            )
            for stage in STAGES
        }

    tasks["bronze"] >> tasks["silver"] >> tasks["gold"] >> tasks["dq"]
