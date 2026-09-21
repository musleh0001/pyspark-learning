from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark_learning.config.settings import settings
from pyspark_learning.schemas.customer import CUSTOMER_SCHEMA
from pyspark_learning.schemas.order import ORDER_SCHEMA
from pyspark_learning.spark.session import create_spark_session


def run():
    spark = create_spark_session()

    try:
        customers = (
            spark.read.schema(CUSTOMER_SCHEMA)
            .option("header", True)
            .csv(settings.input_data_path)
        )

        orders = (
            spark.read.schema(ORDER_SCHEMA)
            .option("header", True)
            .csv("data/raw/orders.csv")
        )

        pivot_df = orders.groupBy("customer_id").pivot("status").agg(F.sum("amount"))
        pivot_df.show()

        window = (
            Window.partitionBy("customer_id")
            .orderBy("order_date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        df = orders.withColumn("running_total", F.sum("amount").over(window))
        df.show()

        window2 = Window.partitionBy("country").orderBy(F.col("total_spent").desc())
        result = customers.withColumn("rank", F.row_number().over(window2)).filter(
            F.col("rank") <= 3
        )
        result.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
