from pyspark.sql import functions as F

from pyspark_learning.config.settings import settings
from pyspark_learning.schemas.customer import CUSTOMER_SCHEMA
from pyspark_learning.spark.session import create_spark_session


def read_customers(spark):
    return (
        spark.read.schema(CUSTOMER_SCHEMA)
        .option("header", True)
        .csv(settings.input_data_path)
    )


def clean_customers(df):
    return (
        df.withColumn(
            "name",
            F.initcap(F.trim(F.col("name"))),
        )
        .withColumn(
            "country",
            F.trim(F.col("country")),
        )
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("total_spent").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def classify_customers(df):
    return df.withColumn(
        "customer_segment",
        F.when(
            F.col("total_spent") >= 3000,
            "high_value",
        )
        .when(
            F.col("total_spent") >= 1500,
            "medium_value",
        )
        .otherwise(
            "low_value",
        ),
    )


def create_country_summary(df):
    return (
        df.groupBy("country")
        .agg(
            F.count("*").alias("customer_count"),
            F.sum("total_spent").alias("total_revenue"),
            F.avg("total_spent").alias("average_spending"),
            F.max("total_spent").alias("maximum_spending"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def run() -> None:
    spark = create_spark_session()

    try:
        customers_df = read_customers(spark)

        print("=== Raw Data ===")
        customers_df.show(truncate=False)

        print("=== Raw Schema ===")
        customers_df.printSchema()

        cleaned_df = clean_customers(customers_df)

        classified_df = classify_customers(cleaned_df)

        print("=== Classified Customers ===")
        classified_df.show(truncate=False)

        country_summary = create_country_summary(classified_df)

        print("=== Country Summary ===")
        country_summary.show(truncate=False)

        (classified_df.write.mode("overwrite").parquet(settings.output_data_path))

        print(f"Customer data written to: {settings.output_data_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    run()
