from pyspark.sql import functions as F

from pyspark_learning.config.settings import settings
from pyspark_learning.readers.kafka_reader import read_orders_stream
from pyspark_learning.spark.session import create_spark_session
from pyspark_learning.transformations.orders import parse_order_events


def run():
    spark = create_spark_session()

    try:
        raw_stream = read_orders_stream(spark)
        orders = parse_order_events(raw_stream)

        orders = orders.withWatermark("updated_at", "10 minutes")

        query = (
            orders.writeStream.format("parquet")
            .outputMode("append")
            .option("path", f"{settings.bronze_path}/orders")
            .option("checkpointLocation", f"{settings.checkpoint_path}/orders")
            .start()
        )

        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
