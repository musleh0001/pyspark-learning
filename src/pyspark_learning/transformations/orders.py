from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_learning.schemas.order import ORDER_SCHEMA


def parse_order_events(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", F.from_json(F.col("payload"), ORDER_SCHEMA))
        .select("event_key", "kafka_timestamp", "data.*")
    )
