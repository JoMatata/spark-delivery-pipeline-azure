from datetime import date

import pytest

from pipeline.gold import courier_performance, daily_zone_kpis, hourly_demand, merchant_ranking

COLS = ("order_id string, courier_id string, vehicle_type string, merchant_id string, merchant_name string, "
        "merchant_category string, merchant_zone string, zone_name string, status string, "
        "basket_value_kes double, delivery_fee_kes double, delivery_time_min double, is_on_time boolean, "
        "distance_km double, order_date date, order_month date, order_hour int, day_of_week int")
D1, D2, MONTH = date(2026, 3, 2), date(2026, 3, 9), date(2026, 3, 1)   # both Mondays


def order(oid, zone="Westlands", status="DELIVERED", basket=100.0, minutes=30.0, on_time=True,
          courier="C1", merchant="M1", d=D1, hour=12, km=2.0):
    delivered = status == "DELIVERED"
    return (oid, courier, "motorbike", merchant, f"Merchant {merchant}", "restaurant", "Westlands", zone, status,
            basket, 150.0, minutes if delivered else None, on_time if delivered else None,
            km, d, MONTH, hour, 1)


@pytest.fixture
def orders(spark):
    rows = [
        order("1", basket=100.0, minutes=20.0, on_time=True),
        order("2", basket=200.0, minutes=40.0, on_time=False),
        order("3", basket=300.0, minutes=30.0, on_time=True),
        order("4", status="CANCELLED", basket=999.0),
        order("5", zone="Karen", basket=500.0, minutes=50.0, on_time=False, merchant="M2", courier="C2"),
        order("6", d=D2, basket=50.0, minutes=25.0, on_time=True, hour=19),
    ]
    return spark.createDataFrame(rows, COLS)


def test_daily_zone_kpis(orders):
    k = {(r.order_date, r.zone_name): r for r in daily_zone_kpis(orders).collect()}
    w = k[(D1, "Westlands")]
    assert w.orders == 4 and w.delivered_orders == 3 and w.cancelled_orders == 1
    assert w.gmv_kes == 600.0                         # cancelled basket excluded from GMV
    assert w.avg_delivery_time_min == 30.0
    assert w.on_time_orders == 2 and w.on_time_pct == pytest.approx(66.67)
    assert w.cancellation_pct == 25.0
    assert w.delivery_fee_revenue_kes == 450.0
    karen = k[(D1, "Karen")]
    assert karen.orders == 1 and karen.on_time_pct == 0.0 and karen.cancellation_pct == 0.0
    assert set(k) == {(D1, "Westlands"), (D1, "Karen"), (D2, "Westlands")}


def test_daily_zone_kpis_reconcile_with_input(orders):
    total = daily_zone_kpis(orders).groupBy().sum("orders", "gmv_kes").first()
    assert total[0] == orders.count()
    assert total[1] == 100 + 200 + 300 + 500 + 50


def test_courier_performance_and_rank_eligibility(orders):
    rows = {r.courier_id: r for r in courier_performance(orders, min_deliveries=3).collect()}
    c1 = rows["C1"]
    assert c1.assigned_orders == 5 and c1.deliveries == 4 and c1.cancellations == 1
    assert c1.active_days == 2 and c1.on_time_pct == 75.0
    assert c1.rank_in_vehicle_class == 1 and c1.is_rank_eligible
    assert rows["C2"].rank_in_vehicle_class is None and not rows["C2"].is_rank_eligible


def test_merchant_ranking(orders):
    rows = {r.merchant_id: r for r in merchant_ranking(orders).collect()}
    assert rows["M1"].gmv_kes == 650.0 and rows["M2"].gmv_kes == 500.0
    assert rows["M1"].gmv_rank_in_zone == 1 and rows["M2"].gmv_rank_in_zone == 2
    assert rows["M1"].gmv_share_of_zone_pct + rows["M2"].gmv_share_of_zone_pct == pytest.approx(100.0)
    assert rows["M1"].cancellation_pct == 20.0


def test_hourly_demand_averages_over_number_of_weekdays(orders):
    h = {(r.zone_name, r.day_of_week, r.order_hour): r for r in hourly_demand(orders).collect()}
    # 4 Westlands orders at 12:00 across 2 distinct Mondays -> 2.0 per Monday
    assert h[("Westlands", 1, 12)].orders == 4
    assert h[("Westlands", 1, 12)].avg_orders_per_day == 2.0
    assert h[("Westlands", 1, 19)].avg_orders_per_day == 0.5
