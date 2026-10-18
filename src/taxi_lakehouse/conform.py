import re
from pyspark.sql import DataFrame, functions as F

from taxi_lakehouse.schema import RENAMES, TARGET_SCHEMA


def conform(df: DataFrame, source_file: str) -> DataFrame:
    # 1. Rename the columns using RENAMES
    for old, new in RENAMES.items():
        df = df.withColumnRenamed(old, new)

    # 2. Cast every column to the type in TARGET_SCHEMA
    for field in TARGET_SCHEMA.fields:
        df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    # 3. Add the lineage columns
    match = re.search(r"(\d{4}-\d{2})", source_file)
    source_month = match.group(1) if match else None

    # df = (
    #     df.withColumn("source_file", F.lit(source_file))
    #     .withColumn("source_month", F.lit(source_month))
    #     .withColumn("ingestion_ts", F.current_timestamp())
    # )

    return df