"""Silver layer data quality cleaning, standardization, feature engineering, and dimension enrichment."""

from typing import Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType


def clean_yellow_trips(
    df: DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_duration_hours: float = 24.0,
    max_distance_miles: float = 500.0,
) -> DataFrame:
    """Cleans raw bronze Yellow Taxi data by applying data quality rules,
    handling missing values, and deriving analytics features.

    Data Quality Rules:
    - pickup_ts and dropoff_ts must not be null
    - dropoff_ts >= pickup_ts
    - trip_duration_minutes between > 0 and max_duration_hours * 60
    - trip_distance between > 0 and max_distance_miles
    - fare_amount > 0 and total_amount > 0
    - pu_location_id and do_location_id must not be null
    - Optional bounds on pickup_ts (e.g. valid month range)
    - Deduplication on core trip identifiers
    """
    # 1. Impute missing/null values with sensible domain defaults
    cleaned_df = (
        df.withColumn(
            "passenger_count",
            F.when(
                F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0),
                F.lit(1),
            )
            .otherwise(F.col("passenger_count"))
            .cast(IntegerType()),
        )
        .withColumn(
            "rate_code_id",
            F.coalesce(F.col("rate_code_id"), F.lit(1)).cast(IntegerType()),
        )
        .withColumn(
            "store_and_fwd_flag",
            F.coalesce(F.col("store_and_fwd_flag"), F.lit("N")),
        )
        .withColumn("extra", F.coalesce(F.col("extra"), F.lit(0.0)))
        .withColumn("mta_tax", F.coalesce(F.col("mta_tax"), F.lit(0.0)))
        .withColumn("tip_amount", F.coalesce(F.col("tip_amount"), F.lit(0.0)))
        .withColumn("tolls_amount", F.coalesce(F.col("tolls_amount"), F.lit(0.0)))
        .withColumn(
            "improvement_surcharge",
            F.coalesce(F.col("improvement_surcharge"), F.lit(0.0)),
        )
        .withColumn(
            "congestion_surcharge",
            F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)),
        )
        .withColumn("airport_fee", F.coalesce(F.col("airport_fee"), F.lit(0.0)))
        .withColumn(
            "cbd_congestion_fee",
            F.coalesce(F.col("cbd_congestion_fee"), F.lit(0.0)),
        )
    )

    # 2. Derive time and trip metrics
    duration_min_col = F.round(
        (F.unix_timestamp(F.col("dropoff_ts")) - F.unix_timestamp(F.col("pickup_ts"))) / 60.0,
        2,
    )

    enriched_df = (
        cleaned_df.withColumn("trip_duration_minutes", duration_min_col)
        .withColumn("trip_date", F.to_date(F.col("pickup_ts")))
        .withColumn("pickup_hour", F.hour(F.col("pickup_ts")))
        .withColumn("pickup_day_name", F.date_format(F.col("pickup_ts"), "EEEE"))
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_ts")))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek(F.col("pickup_ts")).isin(1, 7), F.lit(True)).otherwise(F.lit(False)),
        )
        .withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.round(F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0), 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100.0, 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "payment_type_desc",
            F.when(F.col("payment_type") == 1, "Credit card")
            .when(F.col("payment_type") == 2, "Cash")
            .when(F.col("payment_type") == 3, "No charge")
            .when(F.col("payment_type") == 4, "Dispute")
            .when(F.col("payment_type") == 5, "Unknown")
            .when(F.col("payment_type") == 6, "Voided trip")
            .otherwise("Other"),
        )
        .withColumn(
            "rate_code_desc",
            F.when(F.col("rate_code_id") == 1, "Standard rate")
            .when(F.col("rate_code_id") == 2, "JFK")
            .when(F.col("rate_code_id") == 3, "Newark")
            .when(F.col("rate_code_id") == 4, "Nassau or Westchester")
            .when(F.col("rate_code_id") == 5, "Negotiated fare")
            .when(F.col("rate_code_id") == 6, "Group ride")
            .otherwise("Special/Unknown"),
        )
    )

    # 3. Apply Quality Filters
    max_duration_min = max_duration_hours * 60.0
    quality_conditions = (
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("dropoff_ts") >= F.col("pickup_ts"))
        & (F.col("trip_duration_minutes") > 0)
        & (F.col("trip_duration_minutes") <= max_duration_min)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= max_distance_miles)
        & (F.col("fare_amount") > 0)
        & (F.col("total_amount") > 0)
        & F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
    )

    if start_date is not None:
        quality_conditions = quality_conditions & (F.col("pickup_ts") >= F.lit(start_date))
    if end_date is not None:
        quality_conditions = quality_conditions & (F.col("pickup_ts") <= F.lit(end_date))

    filtered_df = enriched_df.filter(quality_conditions)

    # 4. Deduplicate
    deduped_df = filtered_df.dropDuplicates([
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "pu_location_id",
        "do_location_id",
        "fare_amount",
        "trip_distance",
    ])

    return deduped_df


def enrich_with_zones(df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """Enriches cleaned taxi trips with pickup and dropoff zone metadata
    (Borough, Zone name, service_zone) from the taxi zone reference table.
    """
    pu_lookup = zones_df.select(
        F.col("LocationID").alias("pu_loc_id"),
        F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("pu_borough"),
        F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("pu_zone"),
        F.coalesce(F.col("service_zone"), F.lit("Unknown")).alias("pu_service_zone"),
    )

    do_lookup = zones_df.select(
        F.col("LocationID").alias("do_loc_id"),
        F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("do_borough"),
        F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("do_zone"),
        F.coalesce(F.col("service_zone"), F.lit("Unknown")).alias("do_service_zone"),
    )

    enriched = (
        df.join(pu_lookup, df.pu_location_id == pu_lookup.pu_loc_id, "left")
        .drop("pu_loc_id")
        .join(do_lookup, df.do_location_id == do_lookup.do_loc_id, "left")
        .drop("do_loc_id")
        .withColumn("pu_borough", F.coalesce(F.col("pu_borough"), F.lit("Unknown")))
        .withColumn("pu_zone", F.coalesce(F.col("pu_zone"), F.lit("Unknown")))
        .withColumn("pu_service_zone", F.coalesce(F.col("pu_service_zone"), F.lit("Unknown")))
        .withColumn("do_borough", F.coalesce(F.col("do_borough"), F.lit("Unknown")))
        .withColumn("do_zone", F.coalesce(F.col("do_zone"), F.lit("Unknown")))
        .withColumn("do_service_zone", F.coalesce(F.col("do_service_zone"), F.lit("Unknown")))
        .withColumn("silver_processed_ts", F.current_timestamp())
    )

    return enriched


def build_silver_trips(
    yellow_df: DataFrame,
    zones_df: DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
    """Full pipeline for producing Silver layer trips from Bronze inputs."""
    cleaned = clean_yellow_trips(yellow_df, start_date=start_date, end_date=end_date)
    enriched = enrich_with_zones(cleaned, zones_df)
    return enriched
