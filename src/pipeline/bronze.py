"""Bronze: land raw files as-is into Parquet with explicit schemas + ingestion metadata.

Bronze is append-only raw history. No business logic: the only additions are lineage
columns (_ingested_at, _source_file, _batch_id, _ingest_date) so every downstream row can be
traced back to the exact file and run that produced it.
"""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline import schemas
from pipeline.config import PipelineConfig


def with_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    return (df
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_ingest_date", F.current_date())
            .withColumn("_batch_id", F.lit(batch_id)))


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read
            .option("header", True)
            .option("pathGlobFilter", "*.csv")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schemas.ORDERS_RAW)
            .csv(path))


def read_status_events(spark: SparkSession, path: str) -> DataFrame:
    return (spark.read
            .option("pathGlobFilter", "*.jsonl")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schemas.STATUS_EVENTS_RAW)
            .json(path))


def read_dim_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (spark.read.option("header", True).option("mode", "FAILFAST")
            .option("pathGlobFilter", "*.csv").schema(schema).csv(path))


def run(spark: SparkSession, cfg: PipelineConfig) -> dict:
    batch_id = str(uuid.uuid4())
    landing = cfg.path("landing")
    counts = {}

    # Large sources: partitioned by ingest date so re-runs only replace the current day
    # (spark.sql.sources.partitionOverwriteMode=dynamic).
    for name, reader, src in (("orders", read_orders, f"{landing}/orders"),
                              ("status_events", read_status_events, f"{landing}/status_events")):
        df = with_ingestion_metadata(reader(spark, src), batch_id)
        (df.write.mode("overwrite").partitionBy("_ingest_date")
           .parquet(cfg.path("bronze", name)))
        counts[name] = spark.read.parquet(cfg.path("bronze", name)).count()

    # Small reference data: full snapshot each run.
    for name, schema in (("zones", schemas.ZONES), ("merchants", schemas.MERCHANTS),
                         ("couriers", schemas.COURIERS)):
        df = with_ingestion_metadata(read_dim_csv(spark, f"{landing}/{name}", schema), batch_id)
        df.coalesce(1).write.mode("overwrite").parquet(cfg.path("bronze", name))
        counts[name] = spark.read.parquet(cfg.path("bronze", name)).count()

    return {"batch_id": batch_id, "rows": counts}
