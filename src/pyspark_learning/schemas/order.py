from pyspark.sql import types as T

ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.IntegerType(), False),
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("order_date", T.StringType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("status", T.StringType(), False),
    ]
)
