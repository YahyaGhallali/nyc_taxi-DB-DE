"""Gold layer business aggregations, KPI summaries, route performance, and demand modeling."""

from pyspark.sql import DataFrame, functions as F


def build_daily_metrics(silver_df: DataFrame) -> DataFrame:
    """Aggregates daily operational and financial metrics by pickup borough.
    Optimized for executive reporting, dashboards, and revenue tracking.
    """
    return (
        silver_df.groupBy("trip_date", "pu_borough")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("tolls_amount"), 2).alias("total_tolls"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_min"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.sum(
                F.when(F.col("payment_type_desc") == "Credit card", 1).otherwise(0)
            ).alias("credit_card_trips"),
            F.sum(
                F.when(F.col("payment_type_desc") == "Cash", 1).otherwise(0)
            ).alias("cash_trips"),
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
        .orderBy("trip_date", "pu_borough")
    )


def build_zone_performance(
    silver_df: DataFrame, min_trips: int = 10
) -> DataFrame:
    """Aggregates route corridor volume and economics (Pickup Zone -> Dropoff Zone).
    Identifies high-volume transit routes, revenue hotspots, and airport corridors.
    """
    return (
        silver_df.groupBy("pu_borough", "pu_zone", "do_borough", "do_zone")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_min"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .filter(F.col("total_trips") >= min_trips)
        .withColumn("gold_processed_ts", F.current_timestamp())
        .orderBy(F.col("total_trips").desc())
    )


def build_hourly_demand(silver_df: DataFrame) -> DataFrame:
    """Aggregates demand patterns by hour of day and weekday vs weekend.
    Enables congestion analysis, peak pricing evaluation, and fleet distribution insights.
    """
    return (
        silver_df.groupBy("pickup_hour", "pickup_day_name", "is_weekend")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare_amount"),
            F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_min"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
        .orderBy("is_weekend", "pickup_hour")
    )
