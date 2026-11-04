# Benchmark — local end-to-end run

Run on 2026-10-15 · **5,000,000 distinct orders** (5,073,779 raw rows incl. duplicates) · single machine, Spark local mode.

## Environment

| Item | Value |
|---|---|
| CPU cores available to Spark | 2 |
| RAM (total) | 7.8 GB |
| Spark driver memory | 5g |
| Spark / Python / Java | 4.0.1 / 3.13.16 / 21.0.12.1 |
| Shuffle partitions | 16 (AQE on) |

## Stage runtimes

| Stage | Seconds | Output |
|---|---:|---|
| Data generation (pandas/numpy, not Spark) | 152.7 | 5,073,779 order rows + 1,311,063 JSON events |
| Bronze — CSV/JSON → Parquet + lineage cols | 115.03 | 5,073,779 orders, 1,311,063 events |
| Silver — cast, dedupe, late events, DQ, joins | 388.01 | 4,945,463 clean orders, 54,537 quarantined |
| Gold — 4 aggregate tables | 107.01 | daily_zone_kpis 5,475, courier_performance 30,000, merchant_ranking 24,000, hourly_demand 2,520 |
| DQ gate — 11 reconciliation checks | 26.77 | all passed |
| **Pipeline total (bronze→dq)** | **636.8** | ≈ 7,852 orders/sec |

## Row counts through the layers

| Step | Rows |
|---|---:|
| Raw order rows landed | 5,073,779 |
| Duplicates / stale snapshots removed (window dedupe) | 73,779 |
| Distinct orders | 5,000,000 |
| IN_TRANSIT orders resolved by late-arriving events | 139,020 |
| Quarantined by DQ rules | 54,537 (1.09%) |
| Clean Silver orders | 4,945,463 |
| Raw status events / duplicates dropped / corrupt quarantined | 1,311,063 / 25,922 / 138 |

### DQ rule hits (a row can fail several rules)

| Rule | Rows |
|---|---:|
| `coordinates_out_of_bounds` | 15,215 |
| `invalid_basket_value` | 14,802 |
| `missing_merchant_id` | 7,631 |
| `orphan_courier_id` | 5,077 |
| `invalid_order_ts` | 5,027 |
| `delivered_before_ordered` | 4,562 |
| `unknown_zone` | 2,452 |
| `implausible_delivery_time` | 3 |

## Storage footprint

| Layer | Size (MB) | Files |
|---|---:|---:|
| landing (CSV + JSONL) | 1,255.6 | 29 |
| bronze (Parquet) | 374.4 | 17 |
| silver (Parquet) | 353.6 | 374 |
| gold (Parquet) | 3.8 | 390 |

## Native functions vs UDFs — haversine distance over the full Silver table

Sum of distance over 4,945,463 rows (input cached, so only the expression cost differs):

| Implementation | Seconds | vs native |
|---|---:|---:|
| native Spark SQL functions | 1.41 | 1.0× |
| pandas (Arrow) UDF | 6.19 | 4.4× |
| row-at-a-time Python UDF | 17.47 | 12.4× |

## Notes

- Small container (2 vCPUs, 7.8 GB RAM), Spark local mode: the numbers show relative cost per stage, not cluster throughput. On Azure Databricks the same code scales out by adding workers (shuffle partitions raised to 200 in the `azure` profile).
- A fixed overhead of roughly a minute (JVM start-up, ~365 date partitions written per partitioned table, Parquet footers) is paid regardless of volume, so throughput improves with data size.
- Silver is the most expensive stage by design: it holds the wide shuffles (order de-duplication window on `order_id`, event de-duplication) plus the re-partition by `order_date` for the write. The physical plan shows Spark's `WindowGroupLimit` keeping only the top-1 row per key *before* the shuffle, and the latest-terminal-event table is small enough to be broadcast into the order join.
- Re-generate with `make benchmark` (`python scripts/benchmark.py --rows 5000000`).
