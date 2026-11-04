import math
from datetime import datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from conftest import BBOX
from pipeline import schemas
from pipeline.silver import (apply_dq_rules, apply_status_events, cast_events, cast_orders,
                             dedupe_orders, enrich, haversine_km, latest_terminal_events)

RAW_ORDER_FIELDS = [f.name for f in schemas.ORDERS_RAW.fields]


def with_lineage(schema: StructType) -> StructType:
    # copy first: StructType.add mutates in place
    return (StructType(list(schema.fields)).add("_source_file", "string")
            .add("_ingested_at", "timestamp").add("_ingest_date", "date"))


def raw_orders(spark, rows):
    """Build a Bronze-shaped orders DataFrame (all strings + lineage columns)."""
    base = {f: None for f in RAW_ORDER_FIELDS}
    base.update(order_id="ORD1", customer_id="CUS1", merchant_id="M00001", courier_id="C00001",
                delivery_zone="Westlands", customer_lat="-1.2676", customer_lon="36.8108",
                order_ts="2026-03-02 12:00:00", pickup_ts="2026-03-02 12:15:00",
                delivered_ts="2026-03-02 12:30:00", order_status="DELIVERED", basket_value_kes="1200",
                delivery_fee_kes="150", payment_method="M-Pesa", promised_minutes="35",
                updated_at="2026-03-02 12:31:00")
    data = [{**base, **r, "_source_file": r.get("_source_file", "file:/orders_2026-03.csv"),
             "_ingested_at": datetime(2026, 10, 1, 2, 0), "_ingest_date": datetime(2026, 10, 1).date()}
            for r in rows]
    schema = with_lineage(schemas.ORDERS_RAW)
    return spark.createDataFrame([Row(**d) for d in data], schema=schema)


def test_cast_orders_handles_dirty_values(spark):
    df = cast_orders(raw_orders(spark, [
        {"order_id": "A", "basket_value_kes": "1,250.00"},
        {"order_id": "B", "basket_value_kes": "abc", "order_ts": "2026/13/45 25.61"},
        {"order_id": "C", "order_ts": None, "customer_lat": "not-a-number"},
    ])).orderBy("order_id").collect()
    assert df[0].basket_value_kes == 1250.0
    assert df[0].order_ts == datetime(2026, 3, 2, 12, 0)
    assert df[1].basket_value_kes is None and df[1].order_ts is None   # no ANSI failure, just NULL
    assert df[2].order_ts is None and df[2].customer_lat is None


def test_dedupe_keeps_latest_version_and_collapses_exact_duplicates(spark):
    df = cast_orders(raw_orders(spark, [
        # stale IN_TRANSIT snapshot superseded by the final row
        {"order_id": "A", "order_status": "IN_TRANSIT", "delivered_ts": None, "updated_at": "2026-03-02 12:02:00"},
        {"order_id": "A", "order_status": "DELIVERED", "updated_at": "2026-03-02 12:31:00"},
        # exact duplicates
        {"order_id": "B"}, {"order_id": "B"}, {"order_id": "B"},
        {"order_id": "C"},
    ]))
    out = {r.order_id: r for r in dedupe_orders(df).collect()}
    assert sorted(out) == ["A", "B", "C"]
    assert out["A"].order_status == "DELIVERED"


def test_dedupe_tie_break_prefers_latest_ingestion(spark):
    df = raw_orders(spark, [{"order_id": "A", "basket_value_kes": "100", "_source_file": "a.csv"},
                            {"order_id": "A", "basket_value_kes": "200", "_source_file": "b.csv"}])
    out = dedupe_orders(cast_orders(df)).collect()
    assert len(out) == 1 and out[0].basket_value_kes == 200.0


def _events(spark, rows):
    schema = with_lineage(schemas.STATUS_EVENTS_RAW)
    data = []
    for r in rows:
        d = {"event_id": None, "order_id": None, "event_type": None, "event_ts": None, "received_ts": None,
             "courier_id": "C00001", "payload": None, "app": None, "_corrupt_record": None,
             "_source_file": "e.jsonl", "_ingested_at": datetime(2026, 10, 1), "_ingest_date": datetime(2026, 10, 1).date()}
        d.update(r)
        data.append(Row(**d))
    return cast_events(spark.createDataFrame(data, schema=schema))


def test_latest_terminal_event_wins_and_duplicate_events_dropped(spark):
    ev = _events(spark, [
        {"event_id": "E1", "order_id": "A", "event_type": "ORDER_PICKED_UP",
         "event_ts": "2026-03-02T12:10:00.000+03:00", "received_ts": "2026-03-02T12:10:05.000+03:00"},
        {"event_id": "E2", "order_id": "A", "event_type": "ORDER_DELIVERED",
         "event_ts": "2026-03-02T12:40:00.000+03:00", "received_ts": "2026-03-02T16:40:00.000+03:00"},
        {"event_id": "E2", "order_id": "A", "event_type": "ORDER_DELIVERED",   # at-least-once duplicate
         "event_ts": "2026-03-02T12:40:00.000+03:00", "received_ts": "2026-03-02T16:40:00.000+03:00"},
        {"event_id": "E3", "order_id": "B", "event_type": "ORDER_CANCELLED",
         "event_ts": "2026-03-02T12:05:00.000+03:00", "received_ts": "2026-03-02T12:05:30.000+03:00"},
    ])
    latest = {r.order_id: r for r in latest_terminal_events(ev, late_threshold_min=60).collect()}
    assert len(latest) == 2
    assert latest["A"].event_status == "DELIVERED"
    assert latest["A"].event_status_ts == datetime(2026, 3, 2, 12, 40)
    assert latest["A"].event_arrived_late is True        # received 4h after it happened
    assert latest["B"].event_status == "CANCELLED" and latest["B"].event_arrived_late is False


def test_in_transit_orders_resolved_by_late_event(spark):
    orders = cast_orders(raw_orders(spark, [
        {"order_id": "A", "order_status": "IN_TRANSIT", "delivered_ts": None},
        {"order_id": "B", "order_status": "IN_TRANSIT", "delivered_ts": None},   # no event ever arrives
        {"order_id": "C", "order_status": "DELIVERED"},
    ]))
    events = spark.createDataFrame(
        [("A", "DELIVERED", datetime(2026, 3, 2, 12, 44), True)],
        "order_id string, event_status string, event_status_ts timestamp, event_arrived_late boolean")
    out = {r.order_id: r for r in apply_status_events(orders, events).collect()}
    assert out["A"].status == "DELIVERED" and out["A"].status_source == "late_event"
    assert out["A"].delivered_ts == datetime(2026, 3, 2, 12, 44)
    assert out["B"].status == "IN_TRANSIT" and out["B"].delivered_ts is None
    assert out["C"].status == "DELIVERED" and out["C"].status_source == "order_export"


def py_haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(a))


@pytest.mark.parametrize("a,b", [
    ((-1.2864, 36.8172), (-1.2676, 36.8108)),   # CBD -> Westlands
    ((-1.3197, 36.7073), (-1.2210, 36.8980)),   # Karen -> Kasarani
    ((-1.2864, 36.8172), (-1.2864, 36.8172)),   # same point
])
def test_native_haversine_matches_reference(spark, a, b):
    df = spark.createDataFrame([(*a, *b)], "lat1 double, lon1 double, lat2 double, lon2 double")
    got = df.select(haversine_km("lat1", "lon1", "lat2", "lon2").alias("d")).first().d
    assert got == pytest.approx(py_haversine(*a, *b), abs=1e-9)


def test_cbd_to_westlands_is_about_2_km(spark):
    df = spark.createDataFrame([(-1.2864, 36.8172, -1.2676, 36.8108)], "a double, b double, c double, d double")
    assert 2.0 < df.select(haversine_km("a", "b", "c", "d")).first()[0] < 2.4


def _dims(spark):
    zones = spark.createDataFrame(
        [("Z01", "CBD", -1.2864, 36.8172), ("Z02", "Westlands", -1.2676, 36.8108), ("Z10", "South B", -1.31, 36.835)],
        "zone_id string, zone_name string, centroid_lat double, centroid_lon double")
    merchants = spark.createDataFrame(
        [("M00001", "Simba Grill", "restaurant", "Z02", -1.268, 36.811)],
        "merchant_id string, merchant_name string, category string, zone_id string, merchant_lat double, merchant_lon double")
    couriers = spark.createDataFrame([("C00001", "motorbike")], "courier_id string, vehicle_type string")
    return merchants, couriers, zones


def test_zone_standardisation_and_dim_joins(spark):
    merchants, couriers, zones = _dims(spark)
    orders = spark.createDataFrame(
        [("1", " westlands ", "M00001", "C00001"), ("2", "WESTLANDS", "M00001", None),
         ("3", "Nairobi CBD", "M99999", "C00001"), ("4", "south-b", "M00001", "C99999"), ("5", "Thika", None, None)],
        "order_id string, delivery_zone_raw string, merchant_id string, courier_id string")
    out = {r.order_id: r for r in enrich(orders, merchants, couriers, zones, {"nairobi cbd": "CBD"}).collect()}
    assert out["1"].zone_name == "Westlands" and out["2"].zone_name == "Westlands"
    assert out["3"].zone_name == "CBD" and out["3"]._m_found is None          # orphan merchant
    assert out["4"].zone_name == "South B" and out["4"]._c_found is None      # orphan courier
    assert out["5"].zone_id is None                                            # outside coverage
    assert out["1"].merchant_zone == "Westlands" and out["1"].vehicle_type == "motorbike"


def test_dq_rules_flag_each_problem(spark):
    cols = ("order_id string, _corrupt_record string, order_ts timestamp, merchant_id string, _m_found string, "
            "courier_id string, _c_found string, zone_id string, basket_value_kes double, status string, "
            "customer_lat double, customer_lon double, delivery_time_min double")
    ts = datetime(2026, 3, 2, 12)
    good = ("ok", None, ts, "M1", "M1", "C1", "C1", "Z01", 900.0, "DELIVERED", -1.29, 36.81, 30.0)

    def bad(**kw):
        names = [c.split()[0] for c in cols.split(", ")]
        row = dict(zip(names, good))
        row.update(kw)
        return tuple(row[n] for n in names)

    rows = [
        good,
        bad(order_id="no_ts", order_ts=None),
        bad(order_id="neg_basket", basket_value_kes=-10.0),
        bad(order_id="null_basket", basket_value_kes=None),
        bad(order_id="zero_coords", customer_lat=0.0, customer_lon=0.0),
        bad(order_id="swapped", customer_lat=36.81, customer_lon=-1.29),
        bad(order_id="orphan_courier", _c_found=None),
        bad(order_id="no_merchant", merchant_id=None, _m_found=None),
        bad(order_id="time_travel", delivery_time_min=-12.0),
        bad(order_id="too_slow", delivery_time_min=600.0),
        bad(order_id="multi", zone_id=None, basket_value_kes=0.0),
        bad(order_id="cancelled_no_courier", status="CANCELLED", courier_id=None, _c_found=None,
            delivery_time_min=None),
    ]
    out = {r.order_id: set(r.dq_failures) for r in
           apply_dq_rules(spark.createDataFrame(rows, cols), BBOX, 240).collect()}
    assert out["ok"] == set()
    assert out["cancelled_no_courier"] == set()      # legit: cancelled before assignment
    assert out["no_ts"] == {"invalid_order_ts"}
    assert out["neg_basket"] == {"invalid_basket_value"}
    assert out["null_basket"] == {"invalid_basket_value"}
    assert out["zero_coords"] == {"coordinates_out_of_bounds"}
    assert out["swapped"] == {"coordinates_out_of_bounds"}
    assert out["orphan_courier"] == {"orphan_courier_id"}
    assert out["no_merchant"] == {"missing_merchant_id"}
    assert out["time_travel"] == {"delivered_before_ordered"}
    assert out["too_slow"] == {"implausible_delivery_time"}
    assert out["multi"] == {"unknown_zone", "invalid_basket_value"}


def test_corrupt_events_are_flagged(spark):
    from pipeline.silver import event_dq_failures
    ev = _events(spark, [
        {"event_id": "E1", "order_id": "A", "event_type": "ORDER_DELIVERED",
         "event_ts": "2026-03-02T12:40:00.000+03:00", "received_ts": "2026-03-02T12:41:00.000+03:00"},
        {"_corrupt_record": '{"event_id":"E9","order_id":"A","eve'},
    ])
    flagged = ev.withColumn("f", event_dq_failures()).orderBy(F.col("event_id").asc_nulls_last()).collect()
    assert flagged[0].f == []
    assert "corrupt_json" in flagged[1].f
