from pyspark.sql import SparkSession

from pyspark_learning.config.settings import settings
from pyspark_learning.schemas.customer import CUSTOMER_SCHEMA
from pyspark_learning.spark.session import create_spark_session


def run(spark: SparkSession):
    customers = (
        spark.read.schema(CUSTOMER_SCHEMA)
        .option("header", True)
        .csv(f"{settings.input_path}/customers.csv")
    )

    customers.write.mode("overwrite").parquet(f"{settings.bronze_path}/customers")


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        run(spark)
    finally:
        spark.stop()
