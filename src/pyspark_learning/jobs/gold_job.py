from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pyspark_learning.config.settings import settings
from pyspark_learning.spark.session import create_spark_session


def build_customer_metrics(customer: DataFrame, orders: DataFrame) -> DataFrame:
    order_metrics = (
        orders
        .filter(F.col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("average_order_value")
        )
    )

    return (
        customer
        .join(order_metrics, "customer_id", "left")
        .fillna({"order_count": 0, "total_revenue": 0.0})
    )

def run(spark: SparkSession):
    customers = spark.read.parquet(f"{settings.silver_path}/customers")
    orders = spark.read.option("header", True).csv(f"{settings.input_path}/orders.csv")

    result = build_customer_metrics(customers, orders)
    result.write.mode("overwrite").partitionBy("country").parquet(f"{settings.gold_path}/customer_metrics")


if __name__ == "__main__":
    spark = create_spark_session()

    try:
        run(spark)
    finally:
        spark.stop()

