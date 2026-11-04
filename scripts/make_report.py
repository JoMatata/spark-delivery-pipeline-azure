#!/usr/bin/env python
"""Build the README charts and headline insights from the GOLD tables.

Reads Parquet straight from the lake with pandas/pyarrow (gold tables are small - that is the point
of the gold layer), writes docs/images/*.png and docs/insights.json.

    python scripts/make_report.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402,F401
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from pipeline.config import load_config  # noqa: E402

IMG = ROOT / "docs" / "images"
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e0"
BLUE, BLUE_LIGHT = "#2a78d6", "#9ec5f4"
SEQ = LinearSegmentedColormap.from_list("blues", ["#f3f7fd", "#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "text.color": INK, "xtick.color": INK_2,
    "ytick.color": INK_2, "font.size": 10, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "font.family": "DejaVu Sans",
})


def read(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    for c in ("order_date", "order_month"):
        if c in df:
            df[c] = pd.to_datetime(df[c].astype(str))
    return df


def subtitle(ax, text):
    ax.text(0, 1.02, text, transform=ax.transAxes, color=INK_2, fontsize=9.5, va="bottom")


def chart_heatmap(heat: pd.DataFrame) -> None:
    # avg orders per day for each (zone, hour), averaged over all 7 weekdays
    pivot = heat.pivot_table(index="zone_name", columns="order_hour", values="avg_orders_per_day",
                             aggfunc="sum").reindex(columns=range(24)).fillna(0) / 7
    pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).index]
    fig, ax = plt.subplots(figsize=(11, 5.6))
    im = ax.imshow(pivot.values, aspect="auto", cmap=SEQ, interpolation="nearest")
    ax.set_xticks(range(24), [f"{h:02d}" for h in range(24)])
    ax.set_yticks(range(len(pivot)), pivot.index)
    ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xlabel("Hour of day (Nairobi time)")
    ax.set_title("Hourly demand by zone", pad=22)
    subtitle(ax, "Average orders per day in each hour - lunch (12-13h) and dinner (18-20h) peaks city-wide")
    cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    cb.set_label("orders / day", color=INK_2)
    cb.outline.set_visible(False)
    fig.tight_layout()
    fig.savefig(IMG / "hourly_demand_heatmap.png", dpi=130)
    plt.close(fig)


def chart_delivery_time(zone: pd.DataFrame, city_avg: float) -> None:
    z = zone.sort_values("avg_delivery_time_min")
    fig, ax = plt.subplots(figsize=(9, 5.6))
    ax.barh(z.index, z["avg_delivery_time_min"], color=BLUE, height=0.62, zorder=2)
    for y, (v, ot) in enumerate(zip(z["avg_delivery_time_min"], z["on_time_pct"])):
        ax.text(v - 0.6, y, f"{v:.1f} min", va="center", ha="right", fontsize=8.5, color="white", weight="bold")
        ax.text(v + 0.6, y, f"{ot:.0f}% on time", va="center", fontsize=8.5, color=INK_2)
    ax.set_xlim(0, z["avg_delivery_time_min"].max() * 1.25)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Average delivery time, order placed to delivered (minutes)")
    ax.set_title("Average delivery time by zone", pad=22)
    subtitle(ax, f"Delivered orders only, 12 months · city average {city_avg:.1f} min · label = share delivered within promised time")
    fig.tight_layout()
    fig.savefig(IMG / "avg_delivery_time_by_zone.png", dpi=130)
    plt.close(fig)


def chart_daily_trend(daily: pd.Series) -> None:
    roll = daily.rolling(7, center=True).mean()
    fig, ax = plt.subplots(figsize=(11, 4.6))
    ax.plot(daily.index, daily.values, color=BLUE_LIGHT, lw=1, label="Daily orders")
    ax.plot(roll.index, roll.values, color=BLUE, lw=2.2, label="7-day average")
    ax.set_ylim(0, daily.max() * 1.12)
    ax.yaxis.set_major_locator(matplotlib.ticker.MultipleLocator(4000))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False, loc="upper left")
    ax.set_title("Daily orders, Oct 2025 - Sep 2026", pad=22)
    subtitle(ax, "Clean Silver orders aggregated in gold.daily_zone_kpis - weekly weekend peaks, December festive spike, January dip")
    fig.tight_layout()
    fig.savefig(IMG / "daily_orders_trend.png", dpi=130)
    plt.close(fig)


def main():
    cfg = load_config(profile="local")
    gold = lambda t: read(cfg.path("gold", t))  # noqa: E731
    kpis, heat, couriers, merchants = (gold("daily_zone_kpis"), gold("hourly_demand"),
                                       gold("courier_performance"), gold("merchant_ranking"))
    IMG.mkdir(parents=True, exist_ok=True)

    # ---- zone level (weighted properly: sum numerators / denominators, never average averages)
    kpis["dt_x_n"] = kpis["avg_delivery_time_min"] * kpis["delivered_orders"]
    zone = kpis.groupby("zone_name").agg(orders=("orders", "sum"), delivered=("delivered_orders", "sum"),
                                         cancelled=("cancelled_orders", "sum"), on_time=("on_time_orders", "sum"),
                                         gmv=("gmv_kes", "sum"), dt_x_n=("dt_x_n", "sum"))
    zone["avg_delivery_time_min"] = zone["dt_x_n"] / zone["delivered"]
    zone["on_time_pct"] = zone["on_time"] / zone["delivered"] * 100
    zone["cancellation_pct"] = zone["cancelled"] / zone["orders"] * 100
    tot = zone.sum()
    city_avg_dt = tot["dt_x_n"] / tot["delivered"]

    daily = kpis.groupby("order_date")["orders"].sum().sort_index()
    monthly = kpis.groupby(kpis["order_date"].dt.to_period("M"))["orders"].sum()
    month_dt = kpis.groupby(kpis["order_date"].dt.month).apply(
        lambda g: g["dt_x_n"].sum() / g["delivered_orders"].sum())
    rainy = month_dt.loc[[4, 5]].mean()
    dry = month_dt.loc[[1, 2, 6, 7, 8, 9]].mean()

    city_hour = heat.groupby("order_hour")["avg_orders_per_day"].sum() / 7
    dow = heat.groupby("day_of_week")["avg_orders_per_day"].sum()
    hour_dt = heat.assign(w=heat["avg_delivery_time_min"] * heat["orders"]).groupby("order_hour") \
        .apply(lambda g: g["w"].sum() / g["orders"].sum())

    veh = couriers.groupby("vehicle_type").agg(d=("deliveries", "sum"), ot=("on_time_deliveries", "sum"))
    veh["on_time_pct"] = veh["ot"] / veh["d"] * 100
    msum = merchants.groupby("merchant_id")["gmv_kes"].sum().sort_values(ascending=False)
    top10 = msum.head(max(1, len(msum) // 10)).sum() / msum.sum() * 100

    insights = {
        "total_orders": int(tot["orders"]),
        "delivered_orders": int(tot["delivered"]),
        "gmv_kes": round(float(tot["gmv"]), 0),
        "avg_basket_kes": round(float(tot["gmv"] / tot["delivered"]), 0),
        "city_avg_delivery_min": round(float(city_avg_dt), 1),
        "city_on_time_pct": round(float(tot["on_time"] / tot["delivered"] * 100), 1),
        "city_cancellation_pct": round(float(tot["cancelled"] / tot["orders"] * 100), 2),
        "orders_first_month": int(monthly.iloc[0]), "orders_last_month": int(monthly.iloc[-1]),
        "growth_first_to_last_month_pct": round(float((monthly.iloc[-1] / monthly.iloc[0] - 1) * 100), 1),
        "peak_month": str(monthly.idxmax()), "peak_month_orders": int(monthly.max()),
        "december_vs_november_pct": round(float((monthly[pd.Period("2025-12")] / monthly[pd.Period("2025-11")] - 1) * 100), 1),
        "january_vs_december_pct": round(float((monthly[pd.Period("2026-01")] / monthly[pd.Period("2025-12")] - 1) * 100), 1),
        "busiest_hour": int(city_hour.idxmax()), "busiest_hour_orders_per_day": round(float(city_hour.max()), 1),
        "quietest_hour": int(city_hour.idxmin()),
        "busiest_day_of_week": int(dow.idxmax()), "quietest_day_of_week": int(dow.idxmin()),
        "saturday_vs_tuesday_pct": round(float((dow[6] / dow[2] - 1) * 100), 1),
        "slowest_zone": zone["avg_delivery_time_min"].idxmax(),
        "slowest_zone_min": round(float(zone["avg_delivery_time_min"].max()), 1),
        "fastest_zone": zone["avg_delivery_time_min"].idxmin(),
        "fastest_zone_min": round(float(zone["avg_delivery_time_min"].min()), 1),
        "worst_on_time_zone": zone["on_time_pct"].idxmin(),
        "worst_on_time_zone_pct": round(float(zone["on_time_pct"].min()), 1),
        "best_on_time_zone": zone["on_time_pct"].idxmax(),
        "best_on_time_zone_pct": round(float(zone["on_time_pct"].max()), 1),
        "highest_cancellation_zone": zone["cancellation_pct"].idxmax(),
        "highest_cancellation_zone_pct": round(float(zone["cancellation_pct"].max()), 2),
        "rainy_apr_may_avg_delivery_min": round(float(rainy), 1),
        "dry_months_avg_delivery_min": round(float(dry), 1),
        "evening_rush_18h_avg_delivery_min": round(float(hour_dt.loc[18]), 1),
        "mid_afternoon_15h_avg_delivery_min": round(float(hour_dt.loc[15]), 1),
        "on_time_pct_by_vehicle": {k: round(float(v), 1) for k, v in veh["on_time_pct"].items()},
        "top_10pct_merchants_gmv_share_pct": round(float(top10), 1),
        "zone_table": zone[["orders", "avg_delivery_time_min", "on_time_pct", "cancellation_pct", "gmv"]]
        .round(2).sort_values("orders", ascending=False).reset_index().to_dict(orient="records"),
    }

    chart_heatmap(heat)
    chart_delivery_time(zone, city_avg_dt)
    chart_daily_trend(daily)
    (ROOT / "docs" / "insights.json").write_text(json.dumps(insights, indent=2, default=str))
    print(json.dumps({k: v for k, v in insights.items() if k != "zone_table"}, indent=2, default=str))


if __name__ == "__main__":
    main()
