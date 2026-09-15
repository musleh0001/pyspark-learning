from pyspark.sql import functions as F

from pyspark_learning.config.settings import settings
from pyspark_learning.schemas.customer import CUSTOMER_SCHEMA
from pyspark_learning.spark.session import create_spark_session


def run():
    spark = create_spark_session()

    try:
        customers_df = (
            spark.read.schema(CUSTOMER_SCHEMA)
            .option("header", True)
            .csv(settings.input_data_path)
        )

        print("=== Input Data ===")
        customers_df.show()

        print("=== Schema ===")
        customers_df.printSchema()

        high_value_customers = customers_df.filter(F.col("total_spent") >= 2000).select(
            "customer_id", "name", "country", "total_spent"
        )

        print("=== High Value Customers ===")
        high_value_customers.show()

        country_summary = customers_df.groupBy("country").agg(
            F.count("*").alias("customer_count"),
            F.sum("total_spent").alias("total_revenue"),
            F.avg("total_spent").alias("average_spending"),
        )

        print("=== Country Summary ===")
        # country_summary.explain(True)
        country_summary.show()

        country_summary.write.mode("overwrite").parquet(settings.output_data_path)
        print(f"Output written to: {settings.output_data_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
