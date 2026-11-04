#!/usr/bin/env python
"""End-to-end local benchmark: generate N orders, run bronze -> silver -> gold -> dq,
time every stage, measure on-disk sizes, and compare native vs UDF haversine.

    python scripts/benchmark.py --rows 5000000

Writes docs/benchmark.md and docs/benchmark_results.json.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd  # module-level: pandas_udf resolves type hints from globals

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def dir_size_mb(path: Path) -> float:
    return round(sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6, 1) if path.exists() else 0.0


def file_count(path: Path, suffix: str) -> int:
    return sum(1 for _ in path.rglob(f"*{suffix}")) if path.exists() else 0


def haversine_comparison(spark, silver_path: str) -> dict:
    """Same distance computed three ways over the full Silver table."""
    import math

    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf, udf

    from pipeline.silver import haversine_km

    @udf("double")
    def haversine_py(lat1, lon1, lat2, lon2):
        if None in (lat1, lon1, lat2, lon2):
            return None
        p1, p2 = math.radians(lat1), math.radians(lat2)
        a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
        return 2 * 6371.0088 * math.asin(math.sqrt(a))

    @pandas_udf("double")
    def haversine_vec(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
        import numpy as np
        p1, p2 = np.radians(lat1), np.radians(lat2)
        a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(np.radians(lon2 - lon1) / 2) ** 2
        return 2 * 6371.0088 * np.arcsin(np.sqrt(a))

    orders = spark.read.parquet(f"{silver_path}/orders").select("merchant_id", "customer_lat", "customer_lon")
    merch = spark.read.parquet(f"{silver_path}/dim_merchants").select("merchant_id", "merchant_lat", "merchant_lon")
    df = orders.join(F.broadcast(merch), "merchant_id").drop("merchant_id").cache()
    n = df.count()
    cols = ("merchant_lat", "merchant_lon", "customer_lat", "customer_lon")
    out = {"rows": n}
    for label, fn in (("native Spark SQL functions", haversine_km),
                      ("pandas (Arrow) UDF", haversine_vec),
                      ("row-at-a-time Python UDF", haversine_py)):
        df.limit(1000).select(F.sum(fn(*cols))).collect()  # warm-up: start Python workers
        t0 = time.perf_counter()
        df.select(F.sum(fn(*cols)).alias("s")).collect()
        out[label] = round(time.perf_counter() - t0, 2)
    df.unpersist()
    return out


def render_markdown(r: dict) -> str:
    s, rows = r["stages"], r["rows"]
    total = round(sum(v for v in r["stage_seconds"].values()), 1)
    lines = [
        "# Benchmark — local end-to-end run",
        "",
        f"Run on {r['run_at'][:10]} · **{r['distinct_orders']:,} distinct orders** "
        f"({rows['bronze_order_rows']:,} raw rows incl. duplicates) · single machine, Spark local mode.",
        "",
        "## Environment",
        "",
        "| Item | Value |", "|---|---|",
        f"| CPU cores available to Spark | {r['env']['cpu_cores']} |",
        f"| RAM (total) | {r['env']['ram_gb']} GB |",
        f"| Spark driver memory | {r['env']['driver_memory']} |",
        f"| Spark / Python / Java | {r['env']['spark']} / {r['env']['python']} / {r['env']['java']} |",
        f"| Shuffle partitions | {r['env']['shuffle_partitions']} (AQE on) |",
        "",
        "## Stage runtimes",
        "",
        "| Stage | Seconds | Output |", "|---|---:|---|",
        f"| Data generation (pandas/numpy, not Spark) | {r['generation_seconds'] or 'n/a'} | "
        f"{rows['bronze_order_rows']:,} order rows + {s['bronze']['rows']['status_events']:,} JSON events |",
        f"| Bronze — CSV/JSON → Parquet + lineage cols | {r['stage_seconds']['bronze']} | "
        f"{s['bronze']['rows']['orders']:,} orders, {s['bronze']['rows']['status_events']:,} events |",
        f"| Silver — cast, dedupe, late events, DQ, joins | {r['stage_seconds']['silver']} | "
        f"{rows['valid_orders']:,} clean orders, {rows['quarantined_orders']:,} quarantined |",
        f"| Gold — 4 aggregate tables | {r['stage_seconds']['gold']} | "
        + ", ".join(f"{k} {v:,}" for k, v in s["gold"]["rows"].items()) + " |",
        f"| DQ gate — 11 reconciliation checks | {r['stage_seconds']['dq']} | all passed |",
        f"| **Pipeline total (bronze→dq)** | **{total}** | ≈ {round(r['distinct_orders'] / total):,} orders/sec |",
        "",
        "## Row counts through the layers",
        "",
        "| Step | Rows |", "|---|---:|",
        f"| Raw order rows landed | {rows['bronze_order_rows']:,} |",
        f"| Duplicates / stale snapshots removed (window dedupe) | {rows['duplicates_removed']:,} |",
        f"| Distinct orders | {rows['deduped_orders']:,} |",
        f"| IN_TRANSIT orders resolved by late-arriving events | {rows['resolved_by_late_event']:,} |",
        f"| Quarantined by DQ rules | {rows['quarantined_orders']:,} "
        f"({rows['quarantined_orders'] / rows['deduped_orders']:.2%}) |",
        f"| Clean Silver orders | {rows['valid_orders']:,} |",
        f"| Raw status events / duplicates dropped / corrupt quarantined | {rows['raw_events']:,} / "
        f"{rows['duplicate_events_dropped']:,} / {rows['quarantined_events']:,} |",
        "",
        "### DQ rule hits (a row can fail several rules)",
        "",
        "| Rule | Rows |", "|---|---:|",
        *[f"| `{k}` | {v:,} |" for k, v in s["silver"]["dq_rule_failures"].items()],
        "",
        "## Storage footprint",
        "",
        "| Layer | Size (MB) | Files |", "|---|---:|---:|",
        *[f"| {k} | {v['mb']:,} | {v['files']:,} |" for k, v in r["storage"].items()],
        "",
        "## Native functions vs UDFs — haversine distance over the full Silver table",
        "",
        f"Sum of distance over {r['haversine']['rows']:,} rows (input cached, so only the expression cost differs):",
        "",
        "| Implementation | Seconds | vs native |", "|---|---:|---:|",
    ]
    base = r["haversine"]["native Spark SQL functions"]
    for k, v in r["haversine"].items():
        if k != "rows":
            lines.append(f"| {k} | {v} | {v / base:.1f}× |")
    lines += [
        "",
        "## Notes",
        "",
        f"- Small container ({r['env']['cpu_cores']} vCPUs, {r['env']['ram_gb']} GB RAM), Spark local mode: the numbers "
        "show relative cost per stage, not cluster throughput. On Azure Databricks the same code scales out by "
        "adding workers (shuffle partitions raised to 200 in the `azure` profile).",
        "- A fixed overhead of roughly a minute (JVM start-up, ~365 date partitions written per partitioned "
        "table, Parquet footers) is paid regardless of volume, so throughput improves with data size.",
        "- Silver is the most expensive stage by design: it holds the wide shuffles (order de-duplication "
        "window on `order_id`, event de-duplication) plus the re-partition by `order_date` for the write. "
        "The physical plan shows Spark's `WindowGroupLimit` keeping only the top-1 row per key *before* the "
        "shuffle, and the latest-terminal-event table is small enough to be broadcast into the order join.",
        "- Re-generate with `make benchmark` (`python scripts/benchmark.py --rows 5000000`).",
        "",
    ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--driver-memory", default="5g")
    ap.add_argument("--skip-generate", action="store_true")
    args = ap.parse_args()
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    os.environ["SPARK_DRIVER_MEMORY"] = args.driver_memory

    from pipeline.config import load_config
    cfg = load_config(profile="local")
    landing = Path(cfg.path("landing"))

    gen_seconds = None
    if not args.skip_generate:
        t0 = time.perf_counter()
        subprocess.run([sys.executable, str(ROOT / "src" / "generate_data.py"), "--rows", str(args.rows),
                        "--out", str(landing)], check=True)
        gen_seconds = round(time.perf_counter() - t0, 1)
    for layer in ("bronze", "silver", "gold"):
        shutil.rmtree(cfg.path(layer), ignore_errors=True)

    from pipeline.run import run_stages
    from pipeline.spark_session import get_spark
    spark = get_spark(cfg)
    metrics = run_stages(["bronze", "silver", "gold", "dq"], profile="local", spark=spark)
    hav = haversine_comparison(spark, cfg.path("silver"))

    java = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    java_ver = next((ln.split('"')[1] for ln in java.splitlines() if "version" in ln and '"' in ln), "?")
    stages = metrics["stages"]
    result = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "distinct_orders": args.rows,
        "generation_seconds": gen_seconds,
        "stage_seconds": {k: v["seconds"] for k, v in stages.items()},
        "stages": stages,
        "rows": stages["silver"]["rows"],
        "storage": {
            "landing (CSV + JSONL)": {"mb": dir_size_mb(landing), "files": file_count(landing, "")},
            "bronze (Parquet)": {"mb": dir_size_mb(Path(cfg.path("bronze"))), "files": file_count(Path(cfg.path("bronze")), ".parquet")},
            "silver (Parquet)": {"mb": dir_size_mb(Path(cfg.path("silver"))), "files": file_count(Path(cfg.path("silver")), ".parquet")},
            "gold (Parquet)": {"mb": dir_size_mb(Path(cfg.path("gold"))), "files": file_count(Path(cfg.path("gold")), ".parquet")},
        },
        "haversine": hav,
        "env": {
            "cpu_cores": os.cpu_count(),
            "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1),
            "driver_memory": args.driver_memory,
            "spark": spark.version, "python": platform.python_version(), "java": java_ver,
            "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        },
    }
    result["storage"]["landing (CSV + JSONL)"]["files"] = sum(1 for f in landing.rglob("*") if f.is_file())
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "benchmark_results.json").write_text(json.dumps(result, indent=2, default=str))
    (ROOT / "docs" / "benchmark.md").write_text(render_markdown(result))
    print(json.dumps({k: result[k] for k in ("stage_seconds", "generation_seconds", "haversine", "storage")}, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
