"""Gold: business-level aggregates for BI (Power BI / Tableau) and ops dashboards.

  daily_zone_kpis      one row per (order_date, zone)       partitioned by order_date
  courier_performance  one row per (order_month, courier)   partitioned by order_month
  merchant_ranking     one row per (order_month, merchant)  partitioned by order_month
  hourly_demand        one row per (zone, day_of_week, hour) - small, single file

Definitions
  GMV                 sum of basket value (KES) for DELIVERED orders (excludes delivery fees)
  delivery_time_min   order placed -> delivered, DELIVERED orders only
  on_time_pct         % of DELIVERED orders with delivery_time_min <= promised_minutes
  cancellation_pct    cancelled orders / all orders * 100
"""
from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig



def _is(status: str):
    # built lazily: Column objects need an active SparkContext
    return F.col("status") == status


def _delivered(col: str):
    return F.when(_is("DELIVERED"), F.col(col))


def _pct(num, den):
    return F.round(F.when(den > 0, num * 100.0 / den), 2)


def daily_zone_kpis(orders: DataFrame) -> DataFrame:
    agg = orders.groupBy("order_date", "zone_name").agg(
        F.count("*").alias("orders"),
        F.sum(_is("DELIVERED").cast("int")).alias("delivered_orders"),
        F.sum(_is("CANCELLED").cast("int")).alias("cancelled_orders"),
        F.round(F.sum(_delivered("basket_value_kes")), 2).alias("gmv_kes"),
        F.round(F.sum(_delivered("delivery_fee_kes")), 2).alias("delivery_fee_revenue_kes"),
        F.round(F.avg(_delivered("basket_value_kes")), 2).alias("avg_basket_kes"),
        F.round(F.avg(_delivered("delivery_time_min")), 2).alias("avg_delivery_time_min"),
        F.percentile_approx(_delivered("delivery_time_min"), 0.9, 1000).alias("p90_delivery_time_min"),
        F.sum(F.when(_is("DELIVERED") & F.col("is_on_time"), 1).otherwise(0)).alias("on_time_orders"),
        F.round(F.avg(_delivered("distance_km")), 3).alias("avg_distance_km"),
    )
    return (agg
            .withColumn("on_time_pct", _pct(F.col("on_time_orders"), F.col("delivered_orders")))
            .withColumn("cancellation_pct", _pct(F.col("cancelled_orders"), F.col("orders"))))


def courier_performance(orders: DataFrame, min_deliveries: int = 20) -> DataFrame:
    agg = (orders.filter(F.col("courier_id").isNotNull())
           .groupBy("order_month", "courier_id", "vehicle_type").agg(
               F.count("*").alias("assigned_orders"),
               F.sum(_is("DELIVERED").cast("int")).alias("deliveries"),
               F.sum(_is("CANCELLED").cast("int")).alias("cancellations"),
               F.countDistinct("order_date").alias("active_days"),
               F.round(F.avg(_delivered("delivery_time_min")), 2).alias("avg_delivery_time_min"),
               F.sum(F.when(_is("DELIVERED") & F.col("is_on_time"), 1).otherwise(0)).alias("on_time_deliveries"),
               F.round(F.sum(_delivered("distance_km")), 2).alias("total_distance_km"),
           )
           .withColumn("on_time_pct", _pct(F.col("on_time_deliveries"), F.col("deliveries")))
           .withColumn("deliveries_per_active_day", F.round(F.col("deliveries") / F.col("active_days"), 2)))
    eligible = F.col("deliveries") >= min_deliveries
    w = Window.partitionBy("order_month", "vehicle_type").orderBy(
        F.when(eligible, 0).otherwise(1), F.col("on_time_pct").desc(), F.col("avg_delivery_time_min").asc())
    return (agg.withColumn("rank_in_vehicle_class", F.when(eligible, F.rank().over(w)))
               .withColumn("is_rank_eligible", eligible))


def merchant_ranking(orders: DataFrame) -> DataFrame:
    agg = orders.groupBy("order_month", "merchant_id", "merchant_name", "merchant_category", "merchant_zone").agg(
        F.count("*").alias("orders"),
        F.sum(_is("DELIVERED").cast("int")).alias("delivered_orders"),
        F.sum(_is("CANCELLED").cast("int")).alias("cancelled_orders"),
        F.round(F.coalesce(F.sum(_delivered("basket_value_kes")), F.lit(0.0)), 2).alias("gmv_kes"),
        F.round(F.avg(_delivered("delivery_time_min")), 2).alias("avg_delivery_time_min"),
    ).withColumn("cancellation_pct", _pct(F.col("cancelled_orders"), F.col("orders")))
    zone_w = Window.partitionBy("order_month", "merchant_zone")
    return (agg
            .withColumn("gmv_rank_in_zone", F.dense_rank().over(zone_w.orderBy(F.col("gmv_kes").desc())))
            .withColumn("gmv_share_of_zone_pct", _pct(F.col("gmv_kes"), F.sum("gmv_kes").over(zone_w)))
            .withColumn("gmv_percentile_in_category",
                        F.round(F.percent_rank().over(Window.partitionBy("order_month", "merchant_category")
                                                      .orderBy("gmv_kes")), 4)))


def hourly_demand(orders: DataFrame) -> DataFrame:
    """Average orders per (zone, weekday, hour). Divides by how many of that weekday exist in
    the data so partial weeks do not bias the heatmap."""
    days_per_dow = (orders.select("order_date", "day_of_week").distinct()
                    .groupBy("day_of_week").agg(F.count("*").alias("n_days")))
    agg = orders.groupBy("zone_name", "day_of_week", "order_hour").agg(
        F.count("*").alias("orders"),
        F.round(F.avg(_delivered("delivery_time_min")), 2).alias("avg_delivery_time_min"),
        F.round(F.avg(_is("CANCELLED").cast("double")) * 100, 2).alias("cancellation_pct"),
    )
    return (agg.join(F.broadcast(days_per_dow), "day_of_week")
            .withColumn("avg_orders_per_day", F.round(F.col("orders") / F.col("n_days"), 3))
            .drop("n_days"))


def run(spark: SparkSession, cfg: PipelineConfig) -> dict:
    min_deliveries = cfg.pipeline.get("gold", {}).get("min_deliveries_for_courier_rank", 20)
    # Read once, keep only the columns gold needs (column pruning on Parquet), cache because
    # four aggregates scan it.
    orders = (spark.read.parquet(cfg.path("silver", "orders"))
              .select("order_id", "courier_id", "vehicle_type", "merchant_id", "merchant_name",
                      "merchant_category", "merchant_zone", "zone_name", "status", "basket_value_kes",
                      "delivery_fee_kes", "delivery_time_min", "is_on_time", "distance_km",
                      "order_date", "order_month", "order_hour", "day_of_week")
              .persist(StorageLevel.MEMORY_AND_DISK))

    outputs = {
        "daily_zone_kpis": (daily_zone_kpis(orders), "order_date"),
        "courier_performance": (courier_performance(orders, min_deliveries), "order_month"),
        "merchant_ranking": (merchant_ranking(orders), "order_month"),
        "hourly_demand": (hourly_demand(orders), None),
    }
    counts = {}
    for name, (df, part) in outputs.items():
        path = cfg.path("gold", name)
        if part:
            df.repartition(part).write.mode("overwrite").partitionBy(part).parquet(path)
        else:
            df.coalesce(1).write.mode("overwrite").parquet(path)
        counts[name] = spark.read.parquet(path).count()
    orders.unpersist()
    return {"rows": counts}
