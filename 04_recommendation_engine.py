"""
Recommendation Engine
Generates personalized category recommendations for customers
"""

from pyspark.sql.functions import sum, count, col, desc, row_number
from pyspark.sql.window import Window
from pyspark import pipelines as dp

@dp.materialized_view()
def gold_customer_category_recommendations():
    df = spark.table("silver_credit_transactions")
    # Filter out fraudulent transactions
    clean_df = df.filter(col("is_fraud") == "0")
    # Calculate customer spending by category
    customer_category_spend = (
        clean_df
        .groupBy("cc_num", "category")
        .agg(
            sum("amt").alias("total_spent"),
            count("*").alias("txn_count")
        )
    )
    # Rank categories per customer using window function
    window_spec = Window.partitionBy("cc_num").orderBy(desc("total_spent"))
    customer_recs = (
        customer_category_spend
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 3)
        .withColumnRenamed("cc_num", "customer_id")
        .select("customer_id", "category", "total_spent", "txn_count", "rank")
        .orderBy("customer_id", "rank")
    )
    return customer_recs
