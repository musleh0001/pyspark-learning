from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("name", F.initcap(F.col("name")))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )
