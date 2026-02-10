"""
Gold Layer - Analytics Aggregation
Creates business-ready aggregated analytics tables
"""

from pyspark.sql.functions import sum, count, when, col, desc, round
from pyspark import pipelines as dp

@dp.materialized_view()
def gold_spend_by_category():
    df = spark.table("silver_credit_transactions")
    return (
        df.groupBy("category")
          .agg(
              sum("amt").alias("total_spend"),
              count("*").alias("transaction_count")
          )
          .orderBy(desc("total_spend"))
    )

@dp.materialized_view()
def gold_fraud_by_state():
    df = spark.table("silver_credit_transactions")
    return (
        df.groupBy("state")
          .agg(
              count("*").alias("total_transactions"),
              sum(when(col("is_fraud") == "1", 1).otherwise(0)).alias("fraud_transactions")
          )
          .withColumn(
              "fraud_rate_percent",
              round((col("fraud_transactions") / col("total_transactions")) * 100, 2)
          )
          .orderBy(desc("fraud_rate_percent"))
    )

@dp.materialized_view()
def gold_top_merchants():
    df = spark.table("silver_credit_transactions")
    return (
        df.groupBy("merchant")
          .agg(
              sum("amt").alias("total_spend"),
              count("*").alias("transaction_count")
          )
          .orderBy(desc("total_spend"))
          .limit(10)
    )

@dp.materialized_view()
def gold_daily_transaction_trend():
    df = spark.table("silver_credit_transactions")
    return (
        df.groupBy("transaction_date")
          .agg(
              sum("amt").alias("daily_spend"),
              count("*").alias("transaction_count")
          )
          .orderBy("transaction_date")
    )
