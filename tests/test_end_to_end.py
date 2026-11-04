"""Generate a tiny dirty dataset and run every stage against it (temp dirs only)."""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import generate_data  # noqa: E402

from pipeline.run import run_stages  # noqa: E402


def test_generator_is_seeded_and_dirty(tmp_path):
    a = generate_data.main(["--rows", "3000", "--out", str(tmp_path / "a"), "--seed", "7"])
    b = generate_data.main(["--rows", "3000", "--out", str(tmp_path / "b"), "--seed", "7"])
    assert a["raw_order_rows"] == b["raw_order_rows"] > 3000        # duplicates injected
    fa = pd.read_csv(tmp_path / "a" / "orders" / "orders_2026-03.csv")
    fb = pd.read_csv(tmp_path / "b" / "orders" / "orders_2026-03.csv")
    pd.testing.assert_frame_equal(fa, fb)                            # deterministic
    assert len(list((tmp_path / "a" / "orders").glob("*.csv"))) == 12
    assert any((tmp_path / "a" / "status_events").glob("*.jsonl"))


def test_pipeline_end_to_end(spark, tmp_path):
    landing = tmp_path / "landing"
    generate_data.main(["--rows", "4000", "--out", str(landing), "--seed", "11"])
    storage = {"landing": str(landing), "bronze": str(tmp_path / "bronze"), "silver": str(tmp_path / "silver"),
               "gold": str(tmp_path / "gold"), "reports": str(tmp_path / "reports")}
    m = run_stages(["bronze", "silver", "gold", "dq"], profile="local", spark=spark,
                   storage_overrides=storage)["stages"]

    rows = m["silver"]["rows"]
    assert m["bronze"]["rows"]["orders"] > 4000
    assert rows["deduped_orders"] == 4000                            # every duplicate removed
    assert rows["valid_orders"] + rows["quarantined_orders"] == 4000
    assert 0 < rows["quarantined_orders"] < 200
    assert rows["resolved_by_late_event"] > 0
    assert m["dq"]["checks_failed"] == 0

    report = json.loads((tmp_path / "reports" / "dq_report.json").read_text())
    assert report["passed"] is True
    kpis = pd.read_parquet(tmp_path / "gold" / "daily_zone_kpis")
    assert kpis["orders"].sum() == rows["valid_orders"]
    assert kpis["zone_name"].nunique() == 15
    assert kpis["on_time_pct"].dropna().between(0, 100).all()   # NaN = zone-day with no deliveries
