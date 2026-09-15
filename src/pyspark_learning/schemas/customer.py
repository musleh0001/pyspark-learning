import pyspark.sql.types as T

CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.IntegerType(), nullable=False),
        T.StructField("name", T.StringType(), nullable=False),
        T.StructField("country", T.StringType(), nullable=False),
        T.StructField("age", T.IntegerType(), nullable=False),
        T.StructField("total_spent", T.DoubleType(), nullable=False),
    ]
)
