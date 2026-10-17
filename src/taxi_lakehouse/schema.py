from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    StringType, TimestampNTZType,
)

# source column name -> clean name (columns not listed keep their name)
RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}

# clean schema for the 20 source columns (lineage columns are added later)
TARGET_SCHEMA = StructType([
    StructField("vendor_id", IntegerType()),
    StructField("pickup_ts", TimestampNTZType()),
    StructField("dropoff_ts", TimestampNTZType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("rate_code_id", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("pu_location_id", IntegerType()),
    StructField("do_location_id", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])