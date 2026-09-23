from pyspark.sql import SparkSession

from pyspark_learning.config.settings import settings
from pyspark_learning.spark.session import create_spark_session
from pyspark_learning.transformations.customers import clean_customers


def run(spark: SparkSession):
    customers = spark.read.parquet(f"{settings.bronze_path}/customers")

    cleaned = clean_customers(customers)
    cleaned.write.mode("overwrite").partitionBy("country").parquet(
        f"{settings.silver_path}/customers"
    )


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        run(spark)
    finally:
        spark.stop()
