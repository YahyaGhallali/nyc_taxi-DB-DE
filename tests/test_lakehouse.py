from unittest.mock import MagicMock
import pyspark
from pyspark import SparkContext

# Set mock active spark context so F.col / F.lit can construct Column expressions without live JVM
if getattr(SparkContext, "_active_spark_context", None) is None:
    SparkContext._active_spark_context = MagicMock()

from taxi_lakehouse.schema import TARGET_SCHEMA, RENAMES, apply_schema
from taxi_lakehouse.silver import clean_yellow_trips, enrich_with_zones, build_silver_trips
from taxi_lakehouse.gold import build_daily_metrics, build_zone_performance, build_hourly_demand


def test_schema_definitions():
    assert len(TARGET_SCHEMA.fields) == 20
    assert "vendor_id" in [f.name for f in TARGET_SCHEMA.fields]
    assert "pickup_ts" in [f.name for f in TARGET_SCHEMA.fields]
    assert "dropoff_ts" in [f.name for f in TARGET_SCHEMA.fields]
    assert "total_amount" in [f.name for f in TARGET_SCHEMA.fields]
    assert RENAMES["VendorID"] == "vendor_id"
    assert RENAMES["tpep_pickup_datetime"] == "pickup_ts"


def test_apply_schema_mock():
    mock_df = MagicMock()
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    result = apply_schema(mock_df)
    assert result == mock_df
    assert mock_df.withColumnRenamed.call_count == len(RENAMES)
    assert mock_df.withColumn.call_count == len(TARGET_SCHEMA.fields)


def test_clean_yellow_trips_chain():
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df

    result = clean_yellow_trips(
        mock_df,
        start_date="2025-01-01",
        end_date="2025-01-31",
    )
    assert result == mock_df
    assert mock_df.withColumn.called
    assert mock_df.filter.called
    assert mock_df.dropDuplicates.called


def test_enrich_with_zones_chain():
    mock_df = MagicMock()
    mock_zones = MagicMock()
    mock_zones.select.return_value = mock_zones
    mock_df.join.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    result = enrich_with_zones(mock_df, mock_zones)
    assert result == mock_df
    assert mock_zones.select.call_count == 2
    assert mock_df.join.call_count == 2


def test_gold_daily_metrics_chain():
    mock_df = MagicMock()
    mock_grouped = MagicMock()
    mock_df.groupBy.return_value = mock_grouped
    mock_grouped.agg.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.orderBy.return_value = mock_df

    result = build_daily_metrics(mock_df)
    assert result == mock_df
    mock_df.groupBy.assert_called_once_with("trip_date", "pu_borough")


def test_gold_zone_performance_chain():
    mock_df = MagicMock()
    mock_grouped = MagicMock()
    mock_df.groupBy.return_value = mock_grouped
    mock_grouped.agg.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.orderBy.return_value = mock_df

    result = build_zone_performance(mock_df, min_trips=15)
    assert result == mock_df
    mock_df.groupBy.assert_called_once_with("pu_borough", "pu_zone", "do_borough", "do_zone")


def test_gold_hourly_demand_chain():
    mock_df = MagicMock()
    mock_grouped = MagicMock()
    mock_df.groupBy.return_value = mock_grouped
    mock_grouped.agg.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.orderBy.return_value = mock_df

    result = build_hourly_demand(mock_df)
    assert result == mock_df
    mock_df.groupBy.assert_called_once_with("pickup_hour", "pickup_day_name", "is_weekend")
