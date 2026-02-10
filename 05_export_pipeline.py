"""
Data Export for Dashboards and Power BI
Exports Gold layer data in multiple formats and creates Power BI optimized views
"""

from pyspark.sql.functions import *
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_powerbi_views(spark):
    """Create Power BI optimized views"""
    logger.info("Creating Power BI optimized views...")
    
    # View 1: Category Analytics
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_category_analytics AS
        SELECT 
            category,
            CAST(total_spend AS DECIMAL(18,2)) as total_spend,
            transaction_count,
            CAST(total_spend / transaction_count AS DECIMAL(18,2)) as avg_transaction_value
        FROM gold_spend_by_category
        ORDER BY total_spend DESC
    """)
    
    # View 2: Fraud Analytics
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_fraud_analytics AS
        SELECT 
            state,
            total_transactions,
            fraud_transactions,
            CAST(fraud_rate_percent AS DECIMAL(5,2)) as fraud_rate_percent,
            CASE 
                WHEN fraud_rate_percent > 1.0 THEN 'High Risk'
                WHEN fraud_rate_percent > 0.5 THEN 'Medium Risk'
                ELSE 'Low Risk'
            END as risk_category
        FROM gold_fraud_by_state
        ORDER BY fraud_rate_percent DESC
    """)
    
    # View 3: Daily Trends
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_daily_trends AS
        SELECT 
            transaction_date,
            YEAR(transaction_date) as year,
            MONTH(transaction_date) as month,
            DAY(transaction_date) as day,
            DAYOFWEEK(transaction_date) as day_of_week,
            CAST(daily_spend AS DECIMAL(18,2)) as daily_spend,
            transaction_count,
            CAST(daily_spend / transaction_count AS DECIMAL(18,2)) as avg_transaction_value
        FROM gold_daily_transaction_trend
        ORDER BY transaction_date
    """)
    
    # View 4: Top Merchants
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_top_merchants AS
        SELECT 
            merchant,
            CAST(total_spend AS DECIMAL(18,2)) as total_spend,
            transaction_count,
            CAST(total_spend / transaction_count AS DECIMAL(18,2)) as avg_transaction_value
        FROM gold_top_merchants
        ORDER BY total_spend DESC
    """)
    
    # View 5: Customer Recommendations
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_customer_recommendations AS
        SELECT 
            customer_id,
            category,
            CAST(total_spent AS DECIMAL(18,2)) as total_spent,
            txn_count,
            rank as recommendation_rank
        FROM gold_customer_category_recommendations
        WHERE rank <= 3
        ORDER BY customer_id, rank
    """)
    
    logger.info("  ✅ Created 5 Power BI optimized views")


def create_summary_metrics(spark):
    """Create summary metrics table for dashboard KPIs"""
    logger.info("Creating summary metrics table...")
    
    summary_metrics = spark.sql("""
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT cc_num) as total_customers,
            COUNT(DISTINCT merchant) as total_merchants,
            COUNT(DISTINCT category) as total_categories,
            CAST(SUM(CAST(amt AS DOUBLE)) AS DECIMAL(18,2)) as total_revenue,
            CAST(AVG(CAST(amt AS DOUBLE)) AS DECIMAL(18,2)) as avg_transaction_value,
            SUM(CASE WHEN is_fraud = '1' THEN 1 ELSE 0 END) as total_fraud_cases,
            CAST(
                SUM(CASE WHEN is_fraud = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
                AS DECIMAL(5,2)
            ) as overall_fraud_rate_percent
        FROM silver_credit_transactions
    """)
    
    summary_metrics.write.format("delta").mode("overwrite").saveAsTable("gold_summary_metrics")
    
    spark.sql("""
        CREATE OR REPLACE VIEW powerbi_summary_metrics AS
        SELECT * FROM gold_summary_metrics
    """)
    
    logger.info("  ✅ Created gold_summary_metrics table")
    return summary_metrics


def export_to_json(spark, export_path="dbfs:/FileStore/exports"):
    """Export Gold tables to JSON format"""
    logger.info(f"Exporting to JSON at: {export_path}")
    
    # Read Gold tables
    customer_recs = spark.table("gold_customer_category_recommendations")
    category_analytics = spark.table("gold_spend_by_category")
    fraud_analytics = spark.table("gold_fraud_by_state")
    daily_trends = spark.table("gold_daily_transaction_trend")
    top_merchants = spark.table("gold_top_merchants")
    summary_metrics = spark.table("gold_summary_metrics")
    
    # Convert to JSON
    exports = {
        "customer_recommendations.json": customer_recs.limit(100).toPandas().to_json(orient="records", indent=2),
        "category_analytics.json": category_analytics.toPandas().to_json(orient="records", indent=2),
        "fraud_analytics.json": fraud_analytics.toPandas().to_json(orient="records", indent=2),
        "daily_trends.json": daily_trends.limit(100).toPandas().to_json(orient="records", indent=2, date_format="iso"),
        "top_merchants.json": top_merchants.toPandas().to_json(orient="records", indent=2),
        "summary_metrics.json": summary_metrics.toPandas().to_json(orient="records", indent=2)
    }
    
    # Write JSON files
    dbutils.fs.mkdirs(export_path)
    for filename, json_data in exports.items():
        dbutils.fs.put(f"{export_path}/{filename}", json_data, overwrite=True)
        logger.info(f"  ✅ Exported {filename}")
    
    return exports


def run_export_pipeline(spark):
    """
    Run complete export pipeline
    
    Args:
        spark: SparkSession object
    
    Returns:
        dict: Export results
    """
    try:
        logger.info("=" * 80)
        logger.info("Starting Data Export for Dashboards & Power BI")
        logger.info("=" * 80)
        
        # Step 1: Create Power BI views
        logger.info("\n[1/4] Creating Power BI optimized views...")
        create_powerbi_views(spark)
        
        # Step 2: Create summary metrics
        logger.info("\n[2/4] Creating summary metrics...")
        summary_metrics = create_summary_metrics(spark)
        
        # Step 3: Export to JSON
        logger.info("\n[3/4] Exporting to JSON...")
        json_exports = export_to_json(spark)
        
        # Step 4: Verify exports
        logger.info("\n[4/4] Verifying exports...")
        logger.info("\n📊 Delta Tables:")
        spark.sql("SHOW TABLES").filter("tableName LIKE 'gold%'").show(truncate=False)
        
        logger.info("\n📈 Power BI Views:")
        spark.sql("SHOW VIEWS").filter("viewName LIKE 'powerbi%'").show(truncate=False)
        
        logger.info("\n✅ Export pipeline complete")
        logger.info("=" * 80)
        
        # Print Power BI connection guide
        print_powerbi_guide()
        
        return {
            "summary_metrics": summary_metrics,
            "json_exports": json_exports
        }
        
    except Exception as e:
        logger.error(f"❌ Error in export pipeline: {str(e)}")
        raise


def print_powerbi_guide():
    """Print Power BI connection guide"""
    guide = """
    
📈 POWER BI INTEGRATION GUIDE
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


def main():
    """Main execution function"""
    # Run export pipeline
    results = run_export_pipeline(spark)
    
    # Display summary metrics
    logger.info("\n📊 Summary Metrics:")
    results["summary_metrics"].show()
    
    return results


if __name__ == "__main__":
    main()
