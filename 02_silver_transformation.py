"""
Silver Layer - Data Transformation and Feature Engineering
Cleans, transforms, and enriches bronze data into the silver layer
"""

from pyspark.sql.functions import (
    col, to_date, current_timestamp, hour, dayofweek, month, year, avg, count, sqrt, pow
)
from pyspark.sql.window import Window
from pyspark import pipelines as dp

@dp.materialized_view()
def silver_credit_transactions():
    df = spark.table("bronze_credit_transactions")
    df = (
        df
        .dropDuplicates(["transaction_id"])
        .fillna({"merchant": "UNKNOWN", "category": "UNKNOWN"})
        .withColumn("amt", col("amt").cast("double"))
        .withColumn("transaction_date", to_date(col("trans_date_trans_time")))
        .withColumn("processed_timestamp", current_timestamp())
        # Feature engineering
        .withColumn("transaction_hour", hour(col("trans_date_trans_time")))
        .withColumn("transaction_dayofweek", dayofweek(col("trans_date_trans_time")))
        .withColumn("transaction_month", month(col("trans_date_trans_time")))
        .withColumn("transaction_year", year(col("trans_date_trans_time")))
    )

    # Rolling average transaction amount per cardholder (last 5 transactions)
    window_spec = Window.partitionBy("cc_num").orderBy("trans_date_trans_time").rowsBetween(-4, 0)
    df = df.withColumn("rolling_avg_amt_5", avg(col("amt")).over(window_spec))

    # Count transactions per cardholder per day
    window_day = Window.partitionBy(
        "cc_num", "transaction_year", "transaction_month", "transaction_dayofweek"
    )
    df = df.withColumn("txn_count_per_day", count(col("transaction_id")).over(window_day))

    # Distance between cardholder and merchant (Euclidean approximation)
    df = df.withColumn(
        "distance_cardholder_merchant",
        sqrt(
            pow(col("lat") - col("merch_lat"), 2) +
            pow(col("long") - col("merch_long"), 2)
        )
    )

    return df
