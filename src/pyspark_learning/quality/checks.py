from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_STATUSES = {"pending", "completed", "cancelled"}


def split_valid_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .filter(F.col("amount") >= 0)
        .filter(F.col("status").isin(*VALID_STATUSES))
    )

    invalid = df.withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull(), "MISSING_ORDER_ID")
        .when(F.col("customer_id").isNull(), "MISSING_CUSTOMER_ID")
        .when(F.col("amount") < 0, "NEGATIVE_AMOUNT")
        .when(~F.col("status").isin(*VALID_STATUSES), "INVALID_STATUS"),
    ).filter(F.col("rejection_reason").isNotNull())

    return valid, invalid
