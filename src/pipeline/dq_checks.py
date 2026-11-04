"""Post-load data-quality gate. Runs after Gold; any failed check fails the job (and the DAG).

Checks are reconciliation-style ("do the layers agree?") plus invariants ("are values sane?").
A JSON report is written to <reports>/dq_report.json for auditing.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig


class DataQualityError(RuntimeError):
    pass


def _check(results: list, name: str, passed: bool, detail) -> None:
    results.append({"check": name, "passed": bool(passed), "detail": detail})


def run_checks(spark: SparkSession, cfg: PipelineConfig) -> list[dict]:
    dq_conf = cfg.pipeline.get("dq", {})
    silver = spark.read.parquet(cfg.path("silver", "orders"))
    quarantine = spark.read.parquet(cfg.path("silver", "quarantine", "orders"))
    kpis = spark.read.parquet(cfg.path("gold", "daily_zone_kpis"))
    heat = spark.read.parquet(cfg.path("gold", "hourly_demand"))
    merch = spark.read.parquet(cfg.path("gold", "merchant_ranking"))
    results: list[dict] = []

    s = silver.agg(F.count("*").alias("n"), F.countDistinct("order_id").alias("ids"),
                   F.sum(F.when(F.col("status") == "DELIVERED", F.col("basket_value_kes"))).alias("gmv"),
                   F.sum(F.col("zone_name").isNull().cast("int")).alias("null_zone")).first()
    q = quarantine.count()

    _check(results, "silver_not_empty", s["n"] > 0, {"rows": s["n"]})
    _check(results, "silver_order_id_unique", s["n"] == s["ids"], {"rows": s["n"], "distinct": s["ids"]})
    _check(results, "silver_no_null_zone", s["null_zone"] == 0, {"null_zone_rows": s["null_zone"]})
    rate = q / (q + s["n"]) if (q + s["n"]) else 0.0
    _check(results, "quarantine_rate_below_threshold", rate <= dq_conf.get("max_quarantine_rate", 0.05),
           {"quarantined": q, "rate": round(rate, 5), "threshold": dq_conf.get("max_quarantine_rate", 0.05)})

    g = kpis.agg(F.sum("orders").alias("orders"), F.sum("gmv_kes").alias("gmv"),
                 F.min("on_time_pct").alias("min_ot"), F.max("on_time_pct").alias("max_ot"),
                 F.min("cancellation_pct").alias("min_c"), F.max("cancellation_pct").alias("max_c"),
                 F.min("avg_delivery_time_min").alias("min_dt")).first()
    _check(results, "gold_orders_reconcile_with_silver", g["orders"] == s["n"],
           {"gold_orders": g["orders"], "silver_rows": s["n"]})
    gmv_diff = abs((g["gmv"] or 0) - (s["gmv"] or 0))
    _check(results, "gold_gmv_reconciles_with_silver", gmv_diff <= max(1.0, 1e-6 * (s["gmv"] or 0)),
           {"gold_gmv": round(g["gmv"] or 0, 2), "silver_gmv": round(s["gmv"] or 0, 2)})
    _check(results, "on_time_pct_in_range", 0 <= (g["min_ot"] or 0) and (g["max_ot"] or 0) <= 100,
           {"min": g["min_ot"], "max": g["max_ot"]})
    _check(results, "cancellation_pct_in_range", 0 <= (g["min_c"] or 0) and (g["max_c"] or 0) <= 100,
           {"min": g["min_c"], "max": g["max_c"]})
    _check(results, "avg_delivery_time_positive", g["min_dt"] is None or g["min_dt"] > 0, {"min": g["min_dt"]})

    m_orders = merch.agg(F.sum("orders")).first()[0]
    _check(results, "merchant_ranking_reconciles", m_orders == s["n"], {"merchant_orders": m_orders})
    heat_zones = heat.select("zone_name").distinct().count()
    silver_zones = silver.select("zone_name").distinct().count()
    _check(results, "heatmap_covers_all_zones", heat_zones == silver_zones,
           {"heatmap_zones": heat_zones, "silver_zones": silver_zones})
    return results


def run(spark: SparkSession, cfg: PipelineConfig) -> dict:
    results = run_checks(spark, cfg)
    failed = [r for r in results if not r["passed"]]
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "profile": cfg.profile,
              "passed": not failed, "checks": results}
    payload = json.dumps(report, indent=2, default=str)
    if cfg.is_local:
        from pathlib import Path
        Path(cfg.path("reports")).mkdir(parents=True, exist_ok=True)
        Path(cfg.path("reports", "dq_report.json")).write_text(payload)
    else:  # write through Spark so abfss:// works without extra SDKs
        spark.createDataFrame([(payload,)], "report string").coalesce(1) \
             .write.mode("overwrite").text(cfg.path("reports", "dq_report"))
    if failed:
        raise DataQualityError(f"{len(failed)} DQ check(s) failed: {[r['check'] for r in failed]}")
    return {"checks_passed": len(results), "checks_failed": 0}
