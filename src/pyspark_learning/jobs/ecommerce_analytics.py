from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

from pyspark_learning.schemas.customer import CUSTOMER_SCHEMA
from pyspark_learning.schemas.order import ORDER_SCHEMA
from pyspark_learning.spark.session import create_spark_session


def read_customers(spark: SparkSession) -> DataFrame:
    return (
        spark.read.schema(CUSTOMER_SCHEMA)
        .option("header", True)
        .csv("data/raw/customers.csv")
    )


def read_orders(spark: SparkSession) -> DataFrame:
    return (
        spark.read.schema(ORDER_SCHEMA)
        .option("header", True)
        .csv("data/raw/orders.csv")
    )


def aggregate_orders(orders: DataFrame) -> DataFrame:
    return orders.groupBy("customer_id").agg(
        F.count("*").alias("order_count"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias(
            "completed_order_count"
        ),
        F.sum(
            F.when(F.col("status") == "completed", F.col("amount")).otherwise(0)
        ).alias("total_revenue"),
        F.avg(
            F.when(
                F.col("status") == "completed",
                F.col("amount"),
            )
        ).alias("average_order_value"),
        F.max(F.to_date("order_date")).alias("latest_order_date"),
    )


def add_customer_segment(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "customer_segment",
        F.when(
            F.col("total_revenue") >= 5000,
            "VIP",
        )
        .when(
            F.col("total_revenue") >= 2000,
            "Premium",
        )
        .when(
            F.col("total_revenue") >= 500,
            "Regular",
        )
        .otherwise("Low Value"),
    )


def add_country_rank(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("country").orderBy(F.col("total_revenue").desc())

    return df.withColumn(
        "country_rank",
        F.row_number().over(window),
    )


def build_customer_analytics(customers: DataFrame, orders: DataFrame) -> DataFrame:
    order_metrics = aggregate_orders(orders)

    result = customers.join(
        order_metrics,
        on="customer_id",
        how="left",
    ).fillna(
        {
            "order_count": 0,
            "completed_order_count": 0,
            "total_revenue": 0.0,
        }
    )

    result = add_customer_segment(result)
    result = add_country_rank(result)

    return result


def run():
    spark = create_spark_session()

    try:
        customers = read_customers(spark)
        orders = read_orders(spark)

        analytics = build_customer_analytics(customers, orders)
        analytics.show(truncate=False)

        analytics.write.mode("overwrite").partitionBy("country").parquet(
            "data/output/customer_analytics"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
