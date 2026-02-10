"""
Gold Layer - Analytics Aggregation
Creates business-ready aggregated analytics tables
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spend_by_category(spark, silver_table):
    """Create spend by category aggregation"""
    logger.info("Creating gold_spend_by_category...")
    
    gold_spend_by_category = (
        spark.table(silver_table)
        .groupBy("category")
        .agg(
            sum("amt").alias("total_spend"),
            count("*").alias("transaction_count")
        )
        .orderBy(desc("total_spend"))
    )
    
    gold_spend_by_category.write.format("delta").mode("overwrite").saveAsTable("gold_spend_by_category")
    logger.info(f"  ✅ Created gold_spend_by_category: {gold_spend_by_category.count()} rows")
    
    return gold_spend_by_category


def create_fraud_by_state(spark, silver_table):
    """Create fraud analytics by state"""
    logger.info("Creating gold_fraud_by_state...")
    
    gold_fraud_by_state = (
        spark.table(silver_table)
        .groupBy("state")
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
    
    gold_fraud_by_state.write.format("delta").mode("overwrite").saveAsTable("gold_fraud_by_state")
    logger.info(f"  ✅ Created gold_fraud_by_state: {gold_fraud_by_state.count()} rows")
    
    return gold_fraud_by_state


def create_top_merchants(spark, silver_table):
    """Create top merchants aggregation"""
    logger.info("Creating gold_top_merchants...")
    
    gold_top_merchants = (
        spark.table(silver_table)
        .groupBy("merchant")
        .agg(
            sum("amt").alias("total_spend"),
            count("*").alias("transaction_count")
        )
        .orderBy(desc("total_spend"))
        .limit(10)
    )
    
    gold_top_merchants.write.format("delta").mode("overwrite").saveAsTable("gold_top_merchants")
    logger.info(f"  ✅ Created gold_top_merchants: {gold_top_merchants.count()} rows")
    
    return gold_top_merchants


def create_daily_trends(spark, silver_table):
    """Create daily transaction trends"""
    logger.info("Creating gold_daily_transaction_trend...")
    
    gold_daily_trend = (
        spark.table(silver_table)
        .groupBy("transaction_date")
        .agg(
            sum("amt").alias("daily_spend"),
            count("*").alias("transaction_count")
        )
        .orderBy("transaction_date")
    )
    
    gold_daily_trend.write.format("delta").mode("overwrite").saveAsTable("gold_daily_transaction_trend")
    logger.info(f"  ✅ Created gold_daily_transaction_trend: {gold_daily_trend.count()} rows")
    
    return gold_daily_trend


def run_gold_aggregation(spark, input_table="silver_credit_transactions"):
    """
    Create all Gold layer analytics tables
    
    Args:
        spark: SparkSession object
        input_table: Name of silver Delta table
    
    Returns:
        dict: Dictionary of Gold layer DataFrames
    """
    try:
        logger.info("=" * 70)
        logger.info("Starting Gold Layer ETL - Analytics Aggregation")
        logger.info("=" * 70)
        
        # Read silver table
        logger.info(f"Reading from silver table: {input_table}")
        silver_df = spark.table(input_table)
        logger.info(f"Silver record count: {silver_df.count()}")
        
        # Create all gold tables
        gold_tables = {
            "spend_by_category": create_spend_by_category(spark, input_table),
            "fraud_by_state": create_fraud_by_state(spark, input_table),
            "top_merchants": create_top_merchants(spark, input_table),
            "daily_trends": create_daily_trends(spark, input_table)
        }
        
        logger.info("✅ Gold layer aggregation complete")
        logger.info(f"Created {len(gold_tables)} Gold tables")
        logger.info("=" * 70)
        
        return gold_tables
        
    except Exception as e:
        logger.error(f"❌ Error in Gold layer aggregation: {str(e)}")
        raise


def main():
    """Main execution function"""
    # Configuration
    INPUT_TABLE = "silver_credit_transactions"
    
    # Run aggregation
    gold_tables = run_gold_aggregation(spark, INPUT_TABLE)
    
    # Display sample data from each table
    for name, df in gold_tables.items():
        logger.info(f"\n{name}:")
        df.show(5)
    
    return gold_tables


if __name__ == "__main__":
    main()
