"""CLI entry point. Same code runs locally, in Airflow, and as an Azure Databricks wheel task.

    python -m pipeline.run --stage all --profile local
    python -m pipeline.run --stage silver --profile azure
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

from pipeline import bronze, dq_checks, gold, silver
from pipeline.config import load_config
from pipeline.spark_session import get_spark

STAGES = {"bronze": bronze.run, "silver": silver.run, "gold": gold.run, "dq": dq_checks.run}
log = logging.getLogger("pipeline")


def run_stages(stages: list[str], profile: str | None = None, config: str | None = None,
               spark=None, storage_overrides: dict[str, str] | None = None) -> dict:
    cfg = load_config(config, profile, overrides=storage_overrides)
    spark = spark or get_spark(cfg)
    metrics = {"profile": cfg.profile, "stages": {}}
    for stage in stages:
        log.info("stage=%s profile=%s starting", stage, cfg.profile)
        t0 = time.perf_counter()
        result = STAGES[stage](spark, cfg)
        elapsed = round(time.perf_counter() - t0, 2)
        metrics["stages"][stage] = {"seconds": elapsed, **result}
        log.info("stage=%s finished in %.1fs %s", stage, elapsed, json.dumps(result, default=str))
    if cfg.is_local:
        out = Path(cfg.path("reports"))
        out.mkdir(parents=True, exist_ok=True)
        (out / "run_metrics.json").write_text(json.dumps(metrics, indent=2, default=str))
    return metrics


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Nairobi delivery medallion pipeline")
    ap.add_argument("--stage", choices=[*STAGES, "all"], default="all")
    ap.add_argument("--profile", default=None, help="config profile (local | azure); default from config/env")
    ap.add_argument("--config", default=None, help="path to config.yaml")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    stages = list(STAGES) if args.stage == "all" else [args.stage]
    run_stages(stages, args.profile, args.config)


if __name__ == "__main__":
    main()
