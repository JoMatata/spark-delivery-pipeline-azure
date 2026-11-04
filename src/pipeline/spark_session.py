"""SparkSession factory driven by the config profile."""
from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config import PipelineConfig


def get_spark(cfg: PipelineConfig) -> SparkSession:
    spark_cfg = cfg.spark or {}
    builder = SparkSession.builder.appName(spark_cfg.get("app_name", "nairobi-delivery-medallion"))
    if spark_cfg.get("master"):
        builder = builder.master(spark_cfg["master"])
    for key, value in (spark_cfg.get("conf") or {}).items():
        builder = builder.config(key, str(value))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
