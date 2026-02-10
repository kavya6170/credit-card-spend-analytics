"""
Recommendation Engine
Generates personalized category recommendations for customers
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_recommendation_engine(spark, input_table="silver_credit_transactions",
                              output_table="gold_customer_category_recommendations"):
    """
    Generate top-3 category recommendations per customer
    
    Args:
        spark: SparkSession object
        input_table: Name of silver Delta table
        output_table: Name of output recommendations table
    
    Returns:
        DataFrame: Customer recommendations DataFrame
    """
    try:
        logger.info("=" * 70)
        logger.info("Starting Recommendation Engine - ML-Based Ranking")
        logger.info("=" * 70)
        
        # Read silver table
        logger.info(f"Reading from silver table: {input_table}")
        silver_df = spark.table(input_table)
        logger.info(f"Silver record count: {silver_df.count()}")
        
        # Filter out fraudulent transactions
        logger.info("Filtering out fraudulent transactions...")
        clean_df = silver_df.filter(col("is_fraud") == "0")
        logger.info(f"Clean transactions: {clean_df.count()}")
        
        # Calculate customer spending by category
        logger.info("Calculating customer spending profiles...")
        customer_category_spend = (
            clean_df
            .groupBy("cc_num", "category")
            .agg(
                sum("amt").alias("total_spent"),
                count("*").alias("txn_count")
            )
        )
        
        # Rank categories per customer using window function
        logger.info("Ranking categories per customer...")
        window_spec = Window.partitionBy("cc_num").orderBy(desc("total_spent"))
        
        customer_recs = (
            customer_category_spend
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") <= 3)  # Top 3 recommendations
            .withColumnRenamed("cc_num", "customer_id")
            .select("customer_id", "category", "total_spent", "txn_count", "rank")
            .orderBy("customer_id", "rank")
        )
        
        logger.info(f"Generated recommendations for {customer_recs.select('customer_id').distinct().count()} customers")
        logger.info(f"Total recommendations: {customer_recs.count()}")
        
        # Write to Delta table
        logger.info(f"Writing to Delta table: {output_table}")
        customer_recs.write.format("delta").mode("overwrite").saveAsTable(output_table)
        
        logger.info("✅ Recommendation engine complete")
        logger.info("=" * 70)
        
        return customer_recs
        
    except Exception as e:
        logger.error(f"❌ Error in Recommendation engine: {str(e)}")
        raise


def main():
    """Main execution function"""
    # Configuration
    INPUT_TABLE = "silver_credit_transactions"
    OUTPUT_TABLE = "gold_customer_category_recommendations"
    
    # Run recommendation engine
    recommendations = run_recommendation_engine(spark, INPUT_TABLE, OUTPUT_TABLE)
    
    # Display sample recommendations
    logger.info("\nSample recommendations:")
    recommendations.show(15)
    
    return recommendations


if __name__ == "__main__":
    main()
