#!/usr/bin/env python
"""Generate a seeded, SYNTHETIC Nairobi on-demand delivery dataset.

Writes raw files to the landing zone exactly as an upstream system might drop them:

    data/landing/
      zones/zones.csv                    reference data (15 Nairobi neighbourhoods)
      merchants/merchants.csv            restaurants, grocers, pharmacies, convenience
      couriers/couriers.csv              motorbike / bicycle / car couriers
      orders/orders_YYYY-MM.csv          one CSV export per month (order header + timestamps)
      status_events/events_YYYY-MM.jsonl clickstream-like status events, by month RECEIVED

The data is deliberately dirty so the Silver layer has real work to do:
duplicate exports, stale snapshots superseded later, orders stuck IN_TRANSIT whose final
status only arrives as a late event, nulls, malformed timestamps, out-of-bounds or
swapped coordinates, negative baskets, orphan foreign keys, messy zone spellings,
duplicate and malformed JSON events.

Nothing here is real data. Usage:
    python src/generate_data.py --rows 5000000 --out data/landing --seed 42
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

import numpy as np
import pandas as pd

START = pd.Timestamp("2025-10-01")
N_MONTHS = 12

# name, lat, lon, customer demand weight, merchant density weight, traffic congestion
ZONES = [
    ("CBD", -1.2864, 36.8172, 0.07, 0.14, 1.90),
    ("Westlands", -1.2676, 36.8108, 0.11, 0.15, 1.75),
    ("Parklands", -1.2620, 36.8170, 0.07, 0.07, 1.45),
    ("Kilimani", -1.2921, 36.7856, 0.11, 0.11, 1.55),
    ("Kileleshwa", -1.2800, 36.7800, 0.08, 0.06, 1.40),
    ("Lavington", -1.2780, 36.7690, 0.07, 0.05, 1.35),
    ("Upper Hill", -1.2990, 36.8150, 0.05, 0.06, 1.80),
    ("Karen", -1.3197, 36.7073, 0.06, 0.04, 1.20),
    ("Langata", -1.3480, 36.7560, 0.06, 0.04, 1.30),
    ("South B", -1.3100, 36.8350, 0.06, 0.05, 1.50),
    ("South C", -1.3200, 36.8250, 0.06, 0.05, 1.45),
    ("Embakasi", -1.3200, 36.8960, 0.07, 0.06, 1.60),
    ("Kasarani", -1.2210, 36.8980, 0.05, 0.04, 1.50),
    ("Runda", -1.2170, 36.8070, 0.04, 0.03, 1.20),
    ("Eastleigh", -1.2740, 36.8510, 0.04, 0.05, 1.70),
]

CATEGORIES = np.array(["restaurant", "grocery", "pharmacy", "convenience"])
CAT_P = np.array([0.64, 0.20, 0.08, 0.08])
CAT_PREP_MIN = np.array([14.0, 18.0, 8.0, 7.0])
CAT_PROMISE_BASE = np.array([32.0, 42.0, 30.0, 28.0])
CAT_BASKET_MEDIAN = np.array([1100.0, 2600.0, 1400.0, 650.0])
CAT_BASKET_SIGMA = np.array([0.50, 0.60, 0.70, 0.50])
NAME_A = ["Savanna", "Jacaranda", "Uhuru", "Simba", "Baobab", "Tamu", "Kahawa", "Nyama",
          "Pwani", "Mara", "Kifaru", "Twiga", "Mlima", "Zuri", "Amani", "Jambo"]
NAME_B = {
    "restaurant": ["Grill", "Kitchen", "Eatery", "Bites", "Cafe", "Choma Spot", "Pizzeria"],
    "grocery": ["Fresh Mart", "Grocers", "Supermarket", "Greens"],
    "pharmacy": ["Chemist", "Pharmacy"],
    "convenience": ["Express", "Duka", "Mini-Mart"],
}

VEHICLES = np.array(["motorbike", "bicycle", "car"])
VEHICLE_P = np.array([0.75, 0.15, 0.10])
VEHICLE_KMH = np.array([26.0, 13.0, 20.0])

PAYMENTS = np.array(["M-Pesa", "Card", "Cash"])
PAYMENT_P = np.array([0.72, 0.18, 0.10])

# Relative orders per hour of day (Nairobi local time)
HOURS_WEEKDAY = np.array([0.3, 0.15, 0.1, 0.1, 0.1, 0.2, 0.5, 1.2, 1.8, 1.5, 1.4, 2.2,
                          4.2, 4.6, 3.0, 1.9, 1.8, 2.6, 4.0, 5.2, 5.0, 3.6, 2.0, 0.9])
HOURS_WEEKEND = np.array([0.6, 0.35, 0.2, 0.1, 0.1, 0.1, 0.2, 0.5, 1.0, 1.9, 2.9, 3.4,
                          4.0, 4.2, 3.3, 2.4, 2.2, 2.8, 4.2, 5.4, 5.3, 4.0, 2.6, 1.4])
DOW_FACTOR = np.array([0.92, 0.90, 0.94, 0.98, 1.15, 1.25, 1.18])  # Mon..Sun
MONTH_FACTOR = {1: 0.88, 4: 1.06, 5: 1.05, 11: 1.03, 12: 1.15}     # Jan "Njaanuary" dip, rains, festive
RAIN_FACTOR = {3: 1.06, 4: 1.22, 5: 1.18, 10: 1.05, 11: 1.12, 12: 1.04}  # long & short rains slow riders

ORDER_COLUMNS = [
    "order_id", "customer_id", "merchant_id", "courier_id", "delivery_zone",
    "customer_lat", "customer_lon", "order_ts", "pickup_ts", "delivered_ts",
    "order_status", "basket_value_kes", "delivery_fee_kes", "payment_method",
    "promised_minutes", "updated_at",
]


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def fmt_ts(values: np.ndarray) -> np.ndarray:
    """datetime64[s] -> 'yyyy-MM-dd HH:mm:ss' strings ('' for NaT)."""
    if len(values) == 0:
        return np.array([], dtype=object)
    out = np.datetime_as_string(values.astype("datetime64[s]"), unit="s").astype(object)
    out = np.char.replace(out.astype(str), "T", " ").astype(object)
    out[np.isnat(values)] = ""
    return out


def build_zones() -> pd.DataFrame:
    z = pd.DataFrame(ZONES, columns=["zone_name", "centroid_lat", "centroid_lon",
                                     "demand_weight", "merchant_weight", "congestion_index"])
    z.insert(0, "zone_id", [f"Z{i + 1:02d}" for i in range(len(z))])
    return z


def weighted_pick_within(rng, group_of_rows: np.ndarray, pool_group: np.ndarray,
                         pool_weight: np.ndarray) -> np.ndarray:
    """For each row pick an index of the pool whose group equals the row's group (weighted)."""
    out = np.empty(len(group_of_rows), dtype=np.int64)
    for g in np.unique(group_of_rows):
        rows = np.flatnonzero(group_of_rows == g)
        cand = np.flatnonzero(pool_group == g)
        p = pool_weight[cand] / pool_weight[cand].sum()
        out[rows] = rng.choice(cand, size=len(rows), p=p)
    return out


def build_merchants(rng, n: int, zones: pd.DataFrame) -> pd.DataFrame:
    nz = len(zones)
    zw = zones["merchant_weight"].to_numpy()
    zone_idx = np.concatenate([np.repeat(np.arange(nz), 4),
                               rng.choice(nz, size=n - 4 * nz, p=zw / zw.sum())])
    cat_idx = rng.choice(len(CATEGORIES), size=n, p=CAT_P)
    cat_idx[: 4 * nz] = np.tile(np.arange(4), nz)  # every zone has every category
    names = [f"{NAME_A[rng.integers(len(NAME_A))]} {rng.choice(NAME_B[CATEGORIES[c]])}" for c in cat_idx]
    return pd.DataFrame({
        "merchant_id": [f"M{i + 1:05d}" for i in range(n)],
        "merchant_name": names,
        "category": CATEGORIES[cat_idx],
        "zone_id": zones["zone_id"].to_numpy()[zone_idx],
        "merchant_lat": np.round(zones["centroid_lat"].to_numpy()[zone_idx] + rng.normal(0, 0.006, n), 6),
        "merchant_lon": np.round(zones["centroid_lon"].to_numpy()[zone_idx] + rng.normal(0, 0.006, n), 6),
        "avg_prep_minutes": np.round(CAT_PREP_MIN[cat_idx] * rng.lognormal(0, 0.2, n), 1),
        "onboarded_date": (START - pd.to_timedelta(rng.integers(30, 1500, n), unit="D")).strftime("%Y-%m-%d"),
        # internal-only generation helpers (dropped before writing)
        "_zone_idx": zone_idx, "_cat_idx": cat_idx, "_popularity": rng.lognormal(0, 0.9, n),
    })


def build_couriers(rng, n: int, zones: pd.DataFrame) -> pd.DataFrame:
    nz = len(zones)
    zw = zones["merchant_weight"].to_numpy()
    zone_idx = np.concatenate([np.repeat(np.arange(nz), 5),
                               rng.choice(nz, size=n - 5 * nz, p=zw / zw.sum())])
    veh_idx = rng.choice(len(VEHICLES), size=n, p=VEHICLE_P)
    return pd.DataFrame({
        "courier_id": [f"C{i + 1:05d}" for i in range(n)],
        "vehicle_type": VEHICLES[veh_idx],
        "home_zone_id": zones["zone_id"].to_numpy()[zone_idx],
        "joined_date": (START - pd.to_timedelta(rng.integers(0, 900, n), unit="D")).strftime("%Y-%m-%d"),
        "rating": np.round(np.clip(rng.normal(4.6, 0.25, n), 3.0, 5.0), 2),
        "_zone_idx": zone_idx, "_veh_idx": veh_idx,
        "_skill": np.clip(rng.normal(1.0, 0.12, n), 0.65, 1.4),
        "_activity": rng.pareto(2.5, n) + 0.2,
    })


def allocate_daily_counts(rng, total_rows: int) -> pd.Series:
    days = pd.date_range(START, START + pd.DateOffset(months=N_MONTHS) - pd.Timedelta(days=1), freq="D")
    month_no = ((days.year - START.year) * 12 + days.month - START.month).to_numpy()
    growth = 1.0 + 0.45 * month_no / (N_MONTHS - 1)
    w = growth * DOW_FACTOR[days.dayofweek] * np.array([MONTH_FACTOR.get(m, 1.0) for m in days.month])
    w = w * rng.normal(1.0, 0.04, len(days))
    return pd.Series(rng.multinomial(total_rows, w / w.sum()), index=days)


def simulate_month(rng, day_counts: pd.Series, id_offset: int, zones, merchants, couriers, n_customers):
    """Simulate the 'true' lifecycle of every order placed in one month."""
    n = int(day_counts.sum())
    day = np.repeat(day_counts.index.values.astype("datetime64[D]"), day_counts.values)
    dow = ((day.astype("int64") + 3) % 7)  # 1970-01-01 was a Thursday -> Mon=0
    weekend = dow >= 5
    hour = np.where(weekend,
                    rng.choice(24, n, p=HOURS_WEEKEND / HOURS_WEEKEND.sum()),
                    rng.choice(24, n, p=HOURS_WEEKDAY / HOURS_WEEKDAY.sum()))
    order_ts = day.astype("datetime64[s]") + (hour * 3600 + rng.integers(0, 3600, n)).astype("timedelta64[s]")
    month = int(pd.Timestamp(day[0]).month)

    # customer zone & drop-off point
    dw = zones["demand_weight"].to_numpy()
    zone_idx = rng.choice(len(zones), n, p=dw / dw.sum())
    cust_lat = zones["centroid_lat"].to_numpy()[zone_idx] + rng.normal(0, 0.009, n)
    cust_lon = zones["centroid_lon"].to_numpy()[zone_idx] + rng.normal(0, 0.009, n)

    # merchant: 75% from the same zone, otherwise anywhere in the city (popularity-weighted)
    pop = merchants["_popularity"].to_numpy()
    same = rng.random(n) < 0.75
    m_idx = rng.choice(len(merchants), n, p=pop / pop.sum())
    m_idx[same] = weighted_pick_within(rng, zone_idx[same], merchants["_zone_idx"].to_numpy(), pop)
    cat = merchants["_cat_idx"].to_numpy()[m_idx]
    m_lat = merchants["merchant_lat"].to_numpy()[m_idx]
    m_lon = merchants["merchant_lon"].to_numpy()[m_idx]
    m_zone = merchants["_zone_idx"].to_numpy()[m_idx]

    # courier: 85% from the merchant's zone (activity-weighted)
    act = couriers["_activity"].to_numpy()
    local = rng.random(n) < 0.85
    c_idx = rng.choice(len(couriers), n, p=act / act.sum())
    c_idx[local] = weighted_pick_within(rng, m_zone[local], couriers["_zone_idx"].to_numpy(), act)
    speed = VEHICLE_KMH[couriers["_veh_idx"].to_numpy()[c_idx]]
    skill = couriers["_skill"].to_numpy()[c_idx]

    # lifecycle timings (minutes)
    straight_km = haversine_km(m_lat, m_lon, cust_lat, cust_lon)
    road_km = straight_km * 1.35 + 0.3
    commute = np.isin(hour, [7, 8, 9, 17, 18, 19])
    meal = np.isin(hour, [12, 13, 18, 19, 20])
    peak_traffic = np.where(commute, 1.0, np.where(meal, 0.5, 0.2))
    congestion = 1 + (zones["congestion_index"].to_numpy()[zone_idx] - 1) * peak_traffic
    rain = RAIN_FACTOR.get(month, 1.0)
    assign = rng.exponential(2.5 + 2.5 * meal, n)
    prep = merchants["avg_prep_minutes"].to_numpy()[m_idx] * rng.lognormal(0, 0.30, n)
    travel = road_km / speed * 60 * congestion * rain / skill
    handover = 1.5 + rng.exponential(1.5, n)
    total = assign + prep + travel + handover
    promised = np.round((CAT_PROMISE_BASE[cat] + 3.0 * straight_km) / 5) * 5

    pay_idx = rng.choice(len(PAYMENTS), n, p=PAYMENT_P)
    late_by = np.maximum(0, total - promised)
    logit = -3.7 + 0.9 * (pay_idx == 2) + 0.4 * meal + 0.06 * late_by + 0.25 * (rain > 1.1)
    cancelled = rng.random(n) < 1 / (1 + np.exp(-logit))
    cancel_after = np.minimum(total, rng.uniform(1, 25, n))
    unassigned = cancelled & (cancel_after < assign + 1)

    basket = np.round(CAT_BASKET_MEDIAN[cat] * rng.lognormal(0, CAT_BASKET_SIGMA[cat], n)
                      * np.where(weekend, 1.08, 1.0), -1)
    fee = np.clip(np.round((79 + 22 * road_km) / 10) * 10, 99, 499)

    def plus(minutes):
        return order_ts + np.round(minutes * 60).astype("timedelta64[s]")

    pickup = np.where(cancelled & (cancel_after < assign + prep), np.datetime64("NaT", "s"), plus(assign + prep))
    delivered = np.where(cancelled, np.datetime64("NaT", "s"), plus(total))
    cancel_ts = np.where(cancelled, plus(cancel_after), np.datetime64("NaT", "s"))
    final_ts = np.where(cancelled, cancel_ts, delivered)
    cust = (n_customers * rng.beta(0.7, 2.0, n)).astype(np.int64)

    return pd.DataFrame({
        "order_id": [f"ORD{i:09d}" for i in range(id_offset, id_offset + n)],
        "customer_id": [f"CUS{c:07d}" for c in cust],
        "merchant_id": merchants["merchant_id"].to_numpy()[m_idx],
        "courier_id": np.where(unassigned, None, couriers["courier_id"].to_numpy()[c_idx]),
        "delivery_zone": zones["zone_name"].to_numpy()[zone_idx],
        "customer_lat": np.round(cust_lat, 6),
        "customer_lon": np.round(cust_lon, 6),
        "order_ts": order_ts,
        "pickup_ts": pickup.astype("datetime64[s]"),
        "delivered_ts": delivered.astype("datetime64[s]"),
        "cancel_ts": cancel_ts.astype("datetime64[s]"),
        "final_ts": final_ts.astype("datetime64[s]"),
        "order_status": np.where(cancelled, "CANCELLED", "DELIVERED"),
        "basket_value_kes": basket,
        "delivery_fee_kes": fee,
        "payment_method": PAYMENTS[pay_idx],
        "promised_minutes": promised.astype(int),
    })


def export_month(rng, truth: pd.DataFrame, all_order_ids_sample: list) -> tuple[pd.DataFrame, list[dict]]:
    """Turn the true lifecycle into a dirty CSV export + JSON status events."""
    n = len(truth)
    df = truth.copy()
    df["updated_at"] = df["final_ts"] + pd.to_timedelta(rng.integers(5, 600, n), unit="s")
    events: list[dict] = []

    def ev(order_id, etype, ets, courier, lat, lon, lateness_s):
        events.append({"event_id": None, "order_id": order_id, "event_type": etype, "event_ts": ets,
                       "received_ts": ets + np.timedelta64(int(lateness_s), "s"), "courier_id": courier,
                       "lat": lat, "lon": lon})

    # 1) ~3% of orders were still IN_TRANSIT when the nightly export ran; their final status
    #    arrives later as a (possibly very) late event. ~7% of those never get one.
    in_transit = rng.random(n) < 0.03
    it = df[in_transit]
    has_event = rng.random(len(it)) > 0.07
    late_s = np.minimum(rng.lognormal(np.log(3 * 3600), 1.0, len(it)), 6 * 86400)
    for row, ok, lat_s in zip(it.itertuples(index=False), has_event, late_s):
        if ok:
            etype = "ORDER_DELIVERED" if row.order_status == "DELIVERED" else "ORDER_CANCELLED"
            ev(row.order_id, etype, row.final_ts, row.courier_id, row.customer_lat, row.customer_lon, lat_s)
    df.loc[in_transit, "order_status"] = "IN_TRANSIT"
    df.loc[in_transit, "delivered_ts"] = pd.NaT
    df.loc[in_transit, "updated_at"] = df.loc[in_transit, "pickup_ts"].fillna(df.loc[in_transit, "order_ts"])

    # 2) on-time clickstream for a 12% sample of normally-exported orders (consistent with the CSV)
    sample = df[(~in_transit) & (rng.random(n) < 0.12)]
    for row in sample.itertuples(index=False):
        if not pd.isna(row.pickup_ts):
            ev(row.order_id, "ORDER_PICKED_UP", row.pickup_ts, row.courier_id, None, None, rng.exponential(20))
        etype = "ORDER_DELIVERED" if row.order_status == "DELIVERED" else "ORDER_CANCELLED"
        ev(row.order_id, etype, row.final_ts, row.courier_id, row.customer_lat, row.customer_lon, rng.exponential(30))

    # 3) events for orders we have never seen (orphans)
    for oid in rng.choice(all_order_ids_sample, size=max(1, n // 2000)):
        ts = np.datetime64(truth["order_ts"].iloc[0]) + np.timedelta64(int(rng.integers(0, 86400 * 25)), "s")
        ev("ORD9" + oid[4:], "ORDER_DELIVERED", ts, None, None, None, 30)

    # ---------- dirt in the CSV export ----------
    def frac(p):
        return rng.random(n) < p

    df["order_ts_str"] = fmt_ts(df["order_ts"].to_numpy())
    bad_ts = frac(0.0005)
    df.loc[bad_ts, "order_ts_str"] = df.loc[bad_ts, "order_ts"].dt.strftime("%Y/%d/%m %H.%M")
    df.loc[frac(0.0005), "order_ts_str"] = ""
    df.loc[frac(0.0015), "merchant_id"] = None
    df.loc[frac(0.001), "courier_id"] = "C99999"                       # orphan FK
    basket = df["basket_value_kes"].astype(object)
    basket[frac(0.002)] = None
    neg = frac(0.001)
    basket[neg] = -df.loc[neg, "basket_value_kes"]
    commas = frac(0.01)                                                   # "1,250.00" style exports
    basket[commas] = df.loc[commas, "basket_value_kes"].map(lambda v: f"{v:,.2f}")
    df["basket_value_kes"] = basket
    # bad coordinates: (0,0), swapped lat/lon, or garbage
    bad = np.flatnonzero(frac(0.003))
    kind = rng.integers(0, 3, len(bad))
    lat, lon = df["customer_lat"].to_numpy().copy(), df["customer_lon"].to_numpy().copy()
    lat[bad[kind == 0]], lon[bad[kind == 0]] = 0.0, 0.0
    lat[bad[kind == 1]], lon[bad[kind == 1]] = lon[bad[kind == 1]], lat[bad[kind == 1]]
    lat[bad[kind == 2]] = lat[bad[kind == 2]] * 10
    df["customer_lat"], df["customer_lon"] = lat, lon
    # delivered before ordered (clock skew on courier device)
    skew = frac(0.001) & df["delivered_ts"].notna().to_numpy()
    df.loc[skew, "delivered_ts"] = df.loc[skew, "order_ts"] - pd.to_timedelta(rng.integers(60, 3600, skew.sum()), unit="s")
    # messy zone spelling
    zone = df["delivery_zone"].astype(object).to_numpy().copy()
    m = np.flatnonzero(frac(0.06))
    style = rng.integers(0, 4, len(m))
    zone[m] = [z.lower() if s == 0 else z.upper() if s == 1 else f" {z} " if s == 2 else z.lower().replace(" ", "-")
               for z, s in zip(zone[m], style)]
    zone[(zone == "CBD") & frac(0.2)] = "Nairobi CBD"
    zone[(zone == "Upper Hill") & frac(0.2)] = "Upperhill"
    zone[frac(0.0005)] = "Thika"                                          # outside coverage
    df["delivery_zone"] = zone

    out = pd.DataFrame({
        "order_id": df["order_id"], "customer_id": df["customer_id"], "merchant_id": df["merchant_id"],
        "courier_id": df["courier_id"], "delivery_zone": df["delivery_zone"],
        "customer_lat": df["customer_lat"], "customer_lon": df["customer_lon"],
        "order_ts": df["order_ts_str"], "pickup_ts": fmt_ts(df["pickup_ts"].to_numpy()),
        "delivered_ts": fmt_ts(df["delivered_ts"].to_numpy()), "order_status": df["order_status"],
        "basket_value_kes": df["basket_value_kes"], "delivery_fee_kes": df["delivery_fee_kes"],
        "payment_method": df["payment_method"], "promised_minutes": df["promised_minutes"],
        "updated_at": fmt_ts(df["updated_at"].to_numpy()),
    })

    # stale snapshots (earlier IN_TRANSIT version of an order, superseded by the final row) + exact dupes
    snap_idx = np.flatnonzero(frac(0.01) & ~in_transit)
    snaps = out.iloc[snap_idx].copy()
    snaps["order_status"] = "IN_TRANSIT"
    snaps["delivered_ts"] = ""
    snaps["updated_at"] = fmt_ts(df["order_ts"].to_numpy()[snap_idx] + np.timedelta64(120, "s"))
    dupes = out[frac(0.005)]
    out = pd.concat([out, snaps, dupes], ignore_index=True)
    out = out.sample(frac=1.0, random_state=int(rng.integers(1 << 31))).reset_index(drop=True)
    return out, events


def write_events(events: list[dict], out_dir: Path, rng, seq_start: int) -> int:
    """Serialize events to JSONL files partitioned by RECEIVED month, with duplicates + corrupt lines."""
    if not events:
        return seq_start
    ev = pd.DataFrame(events)
    ev["event_id"] = [f"EVT{i:010d}" for i in range(seq_start, seq_start + len(ev))]
    dup = ev[rng.random(len(ev)) < 0.02]                                   # at-least-once delivery
    ev = pd.concat([ev, dup], ignore_index=True)
    ev["month"] = ev["received_ts"].dt.strftime("%Y-%m")
    ev = ev.sort_values("received_ts")
    for month, grp in ev.groupby("month"):
        lines = []
        for r in grp.itertuples(index=False):
            payload = None if r.lat is None or pd.isna(r.lat) else {"lat": r.lat, "lon": r.lon}
            lines.append(json.dumps({
                "event_id": r.event_id, "order_id": r.order_id, "event_type": r.event_type,
                "event_ts": pd.Timestamp(r.event_ts).strftime("%Y-%m-%dT%H:%M:%S.000+03:00"),
                "received_ts": pd.Timestamp(r.received_ts).strftime("%Y-%m-%dT%H:%M:%S.000+03:00"),
                "courier_id": r.courier_id, "payload": payload,
                "app": {"platform": "android" if rng.random() < 0.83 else "ios",
                        "version": f"4.{rng.integers(8, 14)}.{rng.integers(0, 5)}"},
            }, separators=(",", ":")))
        for i in rng.choice(len(lines), size=max(1, len(lines) // 10000), replace=False):
            lines[i] = lines[i][: len(lines[i]) // 2]                        # truncated / corrupt JSON
        with open(out_dir / f"events_{month}.jsonl", "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return seq_start + len(events)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=50_000, help="number of distinct orders (default 50k for CI)")
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args(argv)

    t0 = time.time()
    rng = np.random.default_rng(args.seed)
    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    for sub in ("zones", "merchants", "couriers", "orders", "status_events"):
        (out / sub).mkdir(parents=True, exist_ok=True)

    zones = build_zones()
    n_merchants = int(np.clip(args.rows // 2500, 200, 2500))
    n_couriers = int(np.clip(args.rows // 2000, 150, 3000))
    merchants = build_merchants(rng, n_merchants, zones)
    couriers = build_couriers(rng, n_couriers, zones)
    n_customers = max(1000, args.rows // 6)

    zones.to_csv(out / "zones" / "zones.csv", index=False)
    merchants.drop(columns=[c for c in merchants if c.startswith("_")]).to_csv(out / "merchants" / "merchants.csv", index=False)
    couriers.drop(columns=[c for c in couriers if c.startswith("_")]).to_csv(out / "couriers" / "couriers.csv", index=False)

    daily = allocate_daily_counts(rng, args.rows)
    offset, evt_seq, raw_rows = 1, 1, 0
    for period, counts in daily.groupby(daily.index.to_period("M")):
        truth = simulate_month(rng, counts, offset, zones, merchants, couriers, n_customers)
        export, events = export_month(rng, truth, truth["order_id"].sample(50, random_state=1).tolist())
        export.to_csv(out / "orders" / f"orders_{period}.csv", index=False)
        evt_seq = write_events(events, out / "status_events", rng, evt_seq)
        offset += len(truth)
        raw_rows += len(export)
        print(f"  {period}: {len(truth):>9,} orders  {len(export):>9,} raw rows  {len(events):>8,} events")

    meta = {"distinct_orders": args.rows, "raw_order_rows": raw_rows, "status_events": evt_seq - 1,
            "merchants": n_merchants, "couriers": n_couriers, "seed": args.seed,
            "seconds": round(time.time() - t0, 1)}
    (out / "_generation_meta.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta))
    return meta


if __name__ == "__main__":
    main()
