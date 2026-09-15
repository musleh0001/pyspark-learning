from pyspark.sql import SparkSession


def test_spark_session():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    try:
        data = [
            (1, "Alice", 100.0),
            (2, "Bob", 200.0),
        ]

        df = spark.createDataFrame(
            data,
            ["id", "name", "amount"],
        )

        assert df.count() == 2
        assert df.filter(df.amount > 100).count() == 1

    finally:
        spark.stop()
