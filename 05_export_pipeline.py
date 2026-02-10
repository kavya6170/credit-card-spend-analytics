"""
Data Export for Dashboards and Power BI
Exports Gold layer data in multiple formats and creates Power BI optimized views
"""

from pyspark.sql.functions import *
from pyspark import pipelines as dp
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Power BI Views
@dp.view()
def powerbi_category_analytics():
    df = spark.table("gold_spend_by_category")
    return df.select(
        "category",
        col("total_spend").cast("decimal(18,2)").alias("total_spend"),
        "transaction_count",
        (col("total_spend") / col("transaction_count")).cast("decimal(18,2)").alias("avg_transaction_value")
    ).orderBy(col("total_spend").desc())

@dp.view()
def powerbi_fraud_analytics():
    df = spark.table("gold_fraud_by_state")
    return df.select(
        "state",
        "total_transactions",
        "fraud_transactions",
        col("fraud_rate_percent").cast("decimal(5,2)").alias("fraud_rate_percent"),
        when(col("fraud_rate_percent") > 1.0, "High Risk")
            .when(col("fraud_rate_percent") > 0.5, "Medium Risk")
            .otherwise("Low Risk")
            .alias("risk_category")
    ).orderBy(col("fraud_rate_percent").desc())

@dp.view()
def powerbi_daily_trends():
    df = spark.table("gold_daily_transaction_trend")
    return df.select(
        "transaction_date",
        year(col("transaction_date")).alias("year"),
        month(col("transaction_date")).alias("month"),
        dayofmonth(col("transaction_date")).alias("day"),
        dayofweek(col("transaction_date")).alias("day_of_week"),
        col("daily_spend").cast("decimal(18,2)").alias("daily_spend"),
        "transaction_count",
        (col("daily_spend") / col("transaction_count")).cast("decimal(18,2)").alias("avg_transaction_value")
    ).orderBy(col("transaction_date"))

@dp.view()
def powerbi_top_merchants():
    df = spark.table("gold_top_merchants")
    return df.select(
        "merchant",
        col("total_spend").cast("decimal(18,2)").alias("total_spend"),
        "transaction_count",
        (col("total_spend") / col("transaction_count")).cast("decimal(18,2)").alias("avg_transaction_value")
    ).orderBy(col("total_spend").desc())

@dp.view()
def powerbi_customer_recommendations():
    df = spark.table("gold_customer_category_recommendations")
    return df.select(
        "customer_id",
        "category",
        col("total_spent").cast("decimal(18,2)").alias("total_spent"),
        "txn_count",
        col("rank").alias("recommendation_rank")
    ).where(col("rank") <= 3).orderBy("customer_id", "rank")

# Summary Metrics Materialized View
@dp.materialized_view()
def gold_summary_metrics():
    df = spark.table("silver_credit_transactions")
    return df.select(
        count("*").alias("total_transactions"),
        countDistinct("cc_num").alias("total_customers"),
        countDistinct("merchant").alias("total_merchants"),
        countDistinct("category").alias("total_categories"),
        sum(col("amt").cast("double")).cast("decimal(18,2)").alias("total_revenue"),
        avg(col("amt").cast("double")).cast("decimal(18,2)").alias("avg_transaction_value"),
        sum(when(col("is_fraud") == "1", 1).otherwise(0)).alias("total_fraud_cases"),
        (sum(when(col("is_fraud") == "1", 1).otherwise(0)) * 100.0 / count("*")).cast("decimal(5,2)").alias("overall_fraud_rate_percent")
    )

@dp.view()
def powerbi_summary_metrics():
    df = spark.table("gold_summary_metrics")
    return df

# Export to JSON (for reference, not executed in pipeline)
def export_to_json(spark, export_path="dbfs:/FileStore/exports"):
    logger.info(f"Exporting to JSON at: {export_path}")
    customer_recs = spark.table("gold_customer_category_recommendations")
    category_analytics = spark.table("gold_spend_by_category")
    fraud_analytics = spark.table("gold_fraud_by_state")
    daily_trends = spark.table("gold_daily_transaction_trend")
    top_merchants = spark.table("gold_top_merchants")
    summary_metrics = spark.table("gold_summary_metrics")
    exports = {
        "customer_recommendations.json": customer_recs.limit(100).toPandas().to_json(orient="records", indent=2),
        "category_analytics.json": category_analytics.toPandas().to_json(orient="records", indent=2),
        "fraud_analytics.json": fraud_analytics.toPandas().to_json(orient="records", indent=2),
        "daily_trends.json": daily_trends.limit(100).toPandas().to_json(orient="records", indent=2, date_format="iso"),
        "top_merchants.json": top_merchants.toPandas().to_json(orient="records", indent=2),
        "summary_metrics.json": summary_metrics.toPandas().to_json(orient="records", indent=2)
    }
    dbutils.fs.mkdirs(export_path)
    for filename, json_data in exports.items():
        dbutils.fs.put(f"{export_path}/{filename}", json_data, overwrite=True)
        logger.info(f"  ✅ Exported {filename}")
    return exports

# Power BI Guide (for reference)
def print_powerbi_guide():
    guide = """
    \n📈 POWER BI INTEGRATION GUIDE
    ================================================================================
    Step 1: Get Databricks Connection Details
      • Server Hostname: <your-workspace>.cloud.databricks.com
      • HTTP Path: Cluster → Advanced Options → JDBC/ODBC
      • Personal Access Token: User Settings → Access Tokens → Generate
    Step 2: Connect Power BI Desktop
      • Open Power BI Desktop
      • Get Data → More → Azure → Azure Databricks
      • Enter Server Hostname and HTTP Path
      • Authentication: Use Personal Access Token
    Step 3: Select Power BI Optimized Views
      ✓ powerbi_category_analytics
      ✓ powerbi_fraud_analytics
      ✓ powerbi_daily_trends
      ✓ powerbi_top_merchants
      ✓ powerbi_customer_recommendations
      ✓ powerbi_summary_metrics
    Step 4: Choose Import Mode
      • DirectQuery: Real-time data (recommended for large datasets)
      • Import: Faster performance (recommended for smaller datasets)
    Step 5: Create Visualizations
      • KPI Cards: total_revenue, total_transactions, overall_fraud_rate
      • Bar Chart: Spend by Category
      • Map: Fraud Rate by State
      • Line Chart: Daily Transaction Trends
      • Table: Top Merchants
      • Matrix: Customer Recommendations
    📊 DATABRICKS SQL DASHBOARD
      • Import Dashboard.lvdash.json in Databricks SQL
      • All Gold tables are ready for visualization
    ================================================================================
    """
    print(guide)
