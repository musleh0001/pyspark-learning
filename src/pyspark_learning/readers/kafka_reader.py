from pyspark.sql import DataFrame, SparkSession

from pyspark_learning.config.settings import settings


def read_orders_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_orders_topic)
        .option("startingOffsets", "latest")
        .load()
    )
