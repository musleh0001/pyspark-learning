from pyspark.sql import SparkSession

from pyspark_learning.config.settings import settings


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.spark_master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    return spark
