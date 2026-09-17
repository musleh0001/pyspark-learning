from pyspark.sql import functions as F

from pyspark_learning.config.settings import settings
from pyspark_learning.spark.session import create_spark_session


def run() -> None:
    spark = create_spark_session()

    try:
        df = spark.read.parquet(settings.output_data_path)
        df.createOrReplaceTempView("customers")

        result = spark.sql("""
            SELECT
                country,
                customer_segment,
                COUNT(*) AS customer_count,
                SUM(total_spent) AS total_revenue
            FROM customers
            GROUP BY
                country,
                customer_segment
            ORDER BY
                total_revenue DESC
        """)

        result.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
