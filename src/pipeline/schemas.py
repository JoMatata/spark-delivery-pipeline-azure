"""Explicit source schemas. Never rely on schema inference for production ingestion:
it costs an extra pass over the data and silently changes types when a file is dirty."""
from pyspark.sql.types import (DateType, DoubleType, StringType, StructField,
                               StructType)

# Orders land as CSV exports from the order-management system. Bronze keeps every column as
# STRING (plus the corrupt-record column) so nothing is lost or silently nulled at ingest;
# typing happens in Silver where failures can be quarantined with a reason.
ORDERS_RAW = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("courier_id", StringType()),
    StructField("delivery_zone", StringType()),
    StructField("customer_lat", StringType()),
    StructField("customer_lon", StringType()),
    StructField("order_ts", StringType()),
    StructField("pickup_ts", StringType()),
    StructField("delivered_ts", StringType()),
    StructField("order_status", StringType()),
    StructField("basket_value_kes", StringType()),
    StructField("delivery_fee_kes", StringType()),
    StructField("payment_method", StringType()),
    StructField("promised_minutes", StringType()),
    StructField("updated_at", StringType()),
    StructField("_corrupt_record", StringType()),
])

STATUS_EVENTS_RAW = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),
    StructField("received_ts", StringType()),
    StructField("courier_id", StringType()),
    StructField("payload", StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])),
    StructField("app", StructType([
        StructField("platform", StringType()),
        StructField("version", StringType()),
    ])),
    StructField("_corrupt_record", StringType()),
])

ZONES = StructType([
    StructField("zone_id", StringType(), False),
    StructField("zone_name", StringType(), False),
    StructField("centroid_lat", DoubleType()),
    StructField("centroid_lon", DoubleType()),
    StructField("demand_weight", DoubleType()),
    StructField("merchant_weight", DoubleType()),
    StructField("congestion_index", DoubleType()),
])

MERCHANTS = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType()),
    StructField("category", StringType()),
    StructField("zone_id", StringType()),
    StructField("merchant_lat", DoubleType()),
    StructField("merchant_lon", DoubleType()),
    StructField("avg_prep_minutes", DoubleType()),
    StructField("onboarded_date", DateType()),
])

COURIERS = StructType([
    StructField("courier_id", StringType(), False),
    StructField("vehicle_type", StringType()),
    StructField("home_zone_id", StringType()),
    StructField("joined_date", DateType()),
    StructField("rating", DoubleType()),
])
