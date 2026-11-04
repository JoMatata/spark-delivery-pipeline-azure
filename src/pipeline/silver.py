"""Silver: typed, deduplicated, validated, conformed orders.

Steps
  1. Cast raw strings to proper types with try_* functions (ANSI-safe: bad values -> NULL, not job failure).
  2. Deduplicate order exports with a window: keep the latest version of each order_id.
  3. Resolve late-arriving status events (orders exported as IN_TRANSIT get their final state).
  4. Conform: standardise zone spellings, broadcast-join merchant / courier / zone dims.
  5. Derive distance_km (native haversine), delivery_time_min, on-time flag, calendar columns.
  6. Evaluate every data-quality rule in ONE pass -> array of failures per row.
     Clean rows -> silver/orders (partitioned by order_date); failures -> silver/quarantine/orders.
"""
from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig

EARTH_RADIUS_KM = 6371.0088
TS_FMT = "yyyy-MM-dd HH:mm:ss"
TERMINAL_EVENTS = {"ORDER_DELIVERED": "DELIVERED", "ORDER_CANCELLED": "CANCELLED"}


def _c(x) -> Column:
    return F.col(x) if isinstance(x, str) else x


def haversine_km(lat1, lon1, lat2, lon2) -> Column:
    """Great-circle distance with native Spark SQL functions (vectorised in the JVM via
    whole-stage codegen; no Python UDF serialisation round-trip)."""
    lat1, lon1, lat2, lon2 = map(_c, (lat1, lon1, lat2, lon2))
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.pow(F.sin(dlat / 2), 2)
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2))
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


def _try_double(col: str, strip_commas: bool = False) -> Column:
    expr = f"regexp_replace({col}, ',', '')" if strip_commas else col
    return F.expr(f"try_cast(trim({expr}) AS DOUBLE)")


def _try_ts(col: str) -> Column:
    return F.try_to_timestamp(F.trim(F.col(col)), F.lit(TS_FMT))


# --------------------------------------------------------------------------- orders
def cast_orders(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("merchant_id").alias("merchant_id"),
        F.trim("courier_id").alias("courier_id"),
        F.col("delivery_zone").alias("delivery_zone_raw"),
        _try_double("customer_lat").alias("customer_lat"),
        _try_double("customer_lon").alias("customer_lon"),
        _try_ts("order_ts").alias("order_ts"),
        _try_ts("pickup_ts").alias("pickup_ts"),
        _try_ts("delivered_ts").alias("delivered_ts"),
        F.upper(F.trim("order_status")).alias("order_status"),
        _try_double("basket_value_kes", strip_commas=True).alias("basket_value_kes"),
        _try_double("delivery_fee_kes", strip_commas=True).alias("delivery_fee_kes"),
        F.trim("payment_method").alias("payment_method"),
        F.expr("try_cast(promised_minutes AS INT)").alias("promised_minutes"),
        _try_ts("updated_at").alias("updated_at"),
        "_corrupt_record", "_source_file", "_ingested_at", "_ingest_date",
    )


def dedupe_latest(df: DataFrame, key: str | list[str], order_by: list[Column]) -> DataFrame:
    """Keep exactly one row per key: the first by `order_by` (row_number, not rank, so exact
    duplicates are also collapsed deterministically)."""
    keys = [key] if isinstance(key, str) else key
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def dedupe_orders(orders: DataFrame) -> DataFrame:
    return dedupe_latest(orders, "order_id", [
        F.col("updated_at").desc_nulls_last(),
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
    ])


# --------------------------------------------------------------------------- events
def cast_events(raw: DataFrame) -> DataFrame:
    return raw.select(
        "event_id", "order_id", F.upper("event_type").alias("event_type"),
        F.try_to_timestamp("event_ts").alias("event_ts"),
        F.try_to_timestamp("received_ts").alias("received_ts"),
        "courier_id",
        F.col("payload.lat").alias("event_lat"), F.col("payload.lon").alias("event_lon"),
        F.col("app.platform").alias("app_platform"), F.col("app.version").alias("app_version"),
        "_corrupt_record", "_source_file", "_ingested_at", "_ingest_date",
    )


def event_dq_failures() -> Column:
    rules = {
        "corrupt_json": F.col("_corrupt_record").isNotNull(),
        "missing_event_id": F.col("event_id").isNull(),
        "missing_order_id": F.col("order_id").isNull(),
        "invalid_event_ts": F.col("event_ts").isNull(),
    }
    return _failures(rules)


def latest_terminal_events(events: DataFrame, late_threshold_min: int = 60) -> DataFrame:
    """At-least-once delivery -> drop duplicate event_ids, then keep the most recent terminal
    event (delivered / cancelled) per order."""
    mapping = F.create_map(*[F.lit(x) for kv in TERMINAL_EVENTS.items() for x in kv])
    terminal = (events.dropDuplicates(["event_id"])
                .filter(F.col("event_type").isin(list(TERMINAL_EVENTS)))
                .withColumn("event_status", mapping[F.col("event_type")]))
    latest = dedupe_latest(terminal, "order_id", [F.col("event_ts").desc(), F.col("received_ts").desc()])
    lag_min = (F.unix_timestamp("received_ts") - F.unix_timestamp("event_ts")) / 60
    return latest.select(
        "order_id", "event_status",
        F.col("event_ts").alias("event_status_ts"),
        (lag_min > late_threshold_min).alias("event_arrived_late"),
    )


def apply_status_events(orders: DataFrame, latest_events: DataFrame) -> DataFrame:
    """Orders exported as IN_TRANSIT take their final status from the event stream."""
    j = orders.join(latest_events, "order_id", "left")
    resolvable = (F.col("order_status") == "IN_TRANSIT") & F.col("event_status").isNotNull()
    status = F.when(resolvable, F.col("event_status")).otherwise(F.col("order_status"))
    return (j
            .withColumn("status_source", F.when(resolvable, F.lit("late_event")).otherwise(F.lit("order_export")))
            .withColumn("status", status)
            .withColumn("delivered_ts",
                        F.when(status == "DELIVERED",
                               F.coalesce("delivered_ts",
                                          F.when(F.col("event_status") == "DELIVERED", F.col("event_status_ts")))))
            .drop("event_status", "event_status_ts"))


# --------------------------------------------------------------------------- conform
def zone_lookup(zones: DataFrame, aliases: dict[str, str]) -> DataFrame:
    """normalised raw spelling -> canonical zone. Includes every canonical name + configured aliases."""
    spark = zones.sparkSession
    canon = zones.select(_norm_zone(F.col("zone_name")).alias("zone_key"), "zone_id", "zone_name")
    alias_df = spark.createDataFrame([(k, v) for k, v in (aliases or {}).items()], "alias string, target string")
    alias_rows = (alias_df.join(zones, alias_df.target == zones.zone_name)
                  .select(_norm_zone(F.col("alias")).alias("zone_key"), "zone_id", "zone_name"))
    return canon.unionByName(alias_rows).dropDuplicates(["zone_key"])


def _norm_zone(col: Column) -> Column:
    return F.lower(F.trim(F.regexp_replace(col, r"[-_\s]+", " ")))


def enrich(orders: DataFrame, merchants: DataFrame, couriers: DataFrame, zones: DataFrame,
           aliases: dict[str, str]) -> DataFrame:
    """Dimensions are tiny (thousands of rows) -> explicit broadcast hash joins: the 5M-row
    fact is never shuffled for these joins."""
    m = merchants.select("merchant_id", F.col("merchant_id").alias("_m_found"), "merchant_name",
                         F.col("category").alias("merchant_category"),
                         F.col("zone_id").alias("merchant_zone_id"), "merchant_lat", "merchant_lon")
    c = couriers.select("courier_id", F.col("courier_id").alias("_c_found"), "vehicle_type")
    z = zone_lookup(zones, aliases)
    mz = zones.select(F.col("zone_id").alias("merchant_zone_id"), F.col("zone_name").alias("merchant_zone"))
    return (orders
            .withColumn("zone_key", _norm_zone(F.col("delivery_zone_raw")))
            .join(F.broadcast(z), "zone_key", "left")
            .join(F.broadcast(m), "merchant_id", "left")
            .join(F.broadcast(mz), "merchant_zone_id", "left")
            .join(F.broadcast(c), "courier_id", "left")
            .drop("zone_key"))


def derive(df: DataFrame) -> DataFrame:
    delivery_min = (F.unix_timestamp("delivered_ts") - F.unix_timestamp("order_ts")) / 60
    return (df
            .withColumn("distance_km", F.round(haversine_km("merchant_lat", "merchant_lon",
                                                            "customer_lat", "customer_lon"), 3))
            .withColumn("delivery_time_min", F.round(delivery_min, 2))
            .withColumn("is_on_time", F.when(F.col("status") == "DELIVERED",
                                             F.col("delivery_time_min") <= F.col("promised_minutes")))
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("order_month", F.trunc("order_ts", "month"))
            .withColumn("order_hour", F.hour("order_ts"))
            # ISO day of week: 1 = Monday ... 7 = Sunday
            .withColumn("day_of_week", ((F.dayofweek("order_ts") + 5) % 7) + 1))


# --------------------------------------------------------------------------- data quality
def _failures(rules: dict[str, Column]) -> Column:
    flagged = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return F.filter(flagged, lambda x: x.isNotNull())


def order_dq_rules(bbox: dict, max_delivery_minutes: float) -> dict[str, Column]:
    lat, lon = F.col("customer_lat"), F.col("customer_lon")
    return {
        "corrupt_record": F.col("_corrupt_record").isNotNull(),
        "missing_order_id": F.col("order_id").isNull(),
        "invalid_order_ts": F.col("order_ts").isNull(),
        "missing_merchant_id": F.col("merchant_id").isNull(),
        "orphan_merchant_id": F.col("merchant_id").isNotNull() & F.col("_m_found").isNull(),
        "orphan_courier_id": F.col("courier_id").isNotNull() & F.col("_c_found").isNull(),
        "unknown_zone": F.col("zone_id").isNull(),
        "invalid_basket_value": F.col("basket_value_kes").isNull() | (F.col("basket_value_kes") <= 0),
        "invalid_status": ~F.col("status").isin("DELIVERED", "CANCELLED", "IN_TRANSIT") | F.col("status").isNull(),
        "coordinates_out_of_bounds": (lat.isNull() | lon.isNull()
                                      | ~lat.between(bbox["min_lat"], bbox["max_lat"])
                                      | ~lon.between(bbox["min_lon"], bbox["max_lon"])),
        "delivered_before_ordered": F.col("delivery_time_min") < 0,
        "implausible_delivery_time": F.col("delivery_time_min") > max_delivery_minutes,
    }


def apply_dq_rules(df: DataFrame, bbox: dict, max_delivery_minutes: float) -> DataFrame:
    return df.withColumn("dq_failures", _failures(order_dq_rules(bbox, max_delivery_minutes)))


SILVER_ORDER_COLUMNS = [
    "order_id", "customer_id", "merchant_id", "merchant_name", "merchant_category", "merchant_zone",
    "courier_id", "vehicle_type", "zone_id", "zone_name", "customer_lat", "customer_lon",
    "order_ts", "pickup_ts", "delivered_ts", "status", "status_source", "event_arrived_late",
    "basket_value_kes", "delivery_fee_kes", "payment_method", "promised_minutes",
    "distance_km", "delivery_time_min", "is_on_time",
    "order_month", "order_hour", "day_of_week", "_source_file", "_ingested_at", "order_date",
]


def transform_orders(orders_raw: DataFrame, events_clean: DataFrame, merchants: DataFrame,
                     couriers: DataFrame, zones: DataFrame, conf: dict) -> DataFrame:
    """Full Silver transformation (pure: DataFrames in, DataFrame with dq_failures out)."""
    dq = conf.get("dq", {})
    orders = dedupe_orders(cast_orders(orders_raw))
    latest = latest_terminal_events(events_clean, dq.get("late_event_threshold_minutes", 60))
    resolved = apply_status_events(orders, latest)
    enriched = derive(enrich(resolved, merchants, couriers, zones, conf.get("zone_aliases", {})))
    return apply_dq_rules(enriched, conf["bbox"], dq.get("max_delivery_minutes", 240))


def run(spark: SparkSession, cfg: PipelineConfig) -> dict:
    conf = cfg.pipeline
    bronze = lambda name: spark.read.parquet(cfg.path("bronze", name))  # noqa: E731

    zones = bronze("zones").drop("_source_file", "_ingested_at", "_ingest_date", "_batch_id")
    merchants = bronze("merchants").drop("_source_file", "_ingested_at", "_ingest_date", "_batch_id")
    couriers = bronze("couriers").drop("_source_file", "_ingested_at", "_ingest_date", "_batch_id")

    # ---- status events: validate, quarantine bad ones
    events = cast_events(bronze("status_events"))
    events = events.withColumn("dq_failures", event_dq_failures()).persist(StorageLevel.MEMORY_AND_DISK)
    events_clean = events.filter(F.size("dq_failures") == 0).drop("dq_failures", "_corrupt_record")
    (events.filter(F.size("dq_failures") > 0)
     .withColumn("_quarantined_at", F.current_timestamp())
     .write.mode("overwrite").partitionBy("_ingest_date").parquet(cfg.path("silver", "quarantine", "status_events")))

    # ---- orders
    checked = transform_orders(bronze("orders"), events_clean, merchants, couriers, zones, conf)
    checked = checked.persist(StorageLevel.MEMORY_AND_DISK)  # consumed by 2 writes + metrics

    valid = checked.filter(F.size("dq_failures") == 0).select(*SILVER_ORDER_COLUMNS)
    (valid.repartition("order_date")          # one file per date partition, no small-file explosion
     .write.mode("overwrite").partitionBy("order_date").parquet(cfg.path("silver", "orders")))

    quarantine = (checked.filter(F.size("dq_failures") > 0)
                  .withColumn("_quarantined_at", F.current_timestamp()))
    (quarantine.coalesce(4).write.mode("overwrite").partitionBy("_ingest_date")
     .parquet(cfg.path("silver", "quarantine", "orders")))

    # ---- conformed dimensions
    zones.write.mode("overwrite").parquet(cfg.path("silver", "dim_zones"))
    merchants.write.mode("overwrite").parquet(cfg.path("silver", "dim_merchants"))
    couriers.write.mode("overwrite").parquet(cfg.path("silver", "dim_couriers"))

    # ---- metrics (cheap: served from the persisted DataFrame)
    summary = checked.agg(
        F.count("*").alias("deduped_orders"),
        F.sum((F.size("dq_failures") == 0).cast("int")).alias("valid_orders"),
        F.sum((F.size("dq_failures") > 0).cast("int")).alias("quarantined_orders"),
        F.sum((F.col("status_source") == "late_event").cast("int")).alias("resolved_by_late_event"),
        F.sum(((F.col("status") == "IN_TRANSIT") & (F.size("dq_failures") == 0)).cast("int")).alias("still_in_transit"),
    ).first().asDict()
    rule_counts = {r["rule"]: r["n"] for r in
                   checked.select(F.explode("dq_failures").alias("rule")).groupBy("rule")
                   .agg(F.count("*").alias("n")).collect()}
    ev_summary = events.agg(
        F.count("*").alias("raw_events"),
        F.sum((F.size("dq_failures") > 0).cast("int")).alias("quarantined_events"),
    ).first().asDict()
    ev_summary["duplicate_events_dropped"] = events_clean.count() - events_clean.dropDuplicates(["event_id"]).count()

    raw_rows = bronze("orders").count()
    checked.unpersist()
    events.unpersist()
    return {"rows": {"bronze_order_rows": raw_rows, **summary,
                     "duplicates_removed": raw_rows - summary["deduped_orders"], **ev_summary},
            "dq_rule_failures": dict(sorted(rule_counts.items(), key=lambda kv: -kv[1]))}
