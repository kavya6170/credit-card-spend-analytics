"""
Silver Layer - Data Transformation
Cleans and transforms bronze data into silver layer
"""

from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_silver_transformation(spark, input_table="bronze_credit_transactions", 
                              output_table="silver_credit_transactions"):
    """
    Transform bronze data into clean silver layer
    
    Args:
        spark: SparkSession object
        input_table: Name of bronze Delta table
        output_table: Name of output silver Delta table
    
    Returns:
        DataFrame: Silver layer DataFrame
    """
    try:
        logger.info("=" * 70)
        logger.info("Starting Silver Layer ETL - Data Transformation")
        logger.info("=" * 70)
        
        # Read bronze table
        logger.info(f"Reading from bronze table: {input_table}")
        bronze_df = spark.table(input_table)
        logger.info(f"Bronze record count: {bronze_df.count()}")
        
        # Apply transformations
        logger.info("Applying data quality transformations...")
        
        silver_df = (
            bronze_df
            # Remove duplicate transactions
            .dropDuplicates(["transaction_id"])
            
            # Handle categorical NULLs
            .fillna({
                "merchant": "UNKNOWN",
                "category": "UNKNOWN"
            })
            
            # Cast amount to double
            .withColumn("amt", col("amt").cast("double"))
            
            # Parse transaction date safely
            .withColumn(
                "transaction_date",
                try_cast(col("trans_date_trans_time"), "date")
            )
            
            # Add processing timestamp
            .withColumn("processed_timestamp", current_timestamp())
        )
        
        # Data quality checks
        initial_count = bronze_df.count()
        final_count = silver_df.count()
        duplicates_removed = initial_count - final_count
        
        logger.info(f"Data quality summary:")
        logger.info(f"  - Initial records: {initial_count}")
        logger.info(f"  - Final records: {final_count}")
        logger.info(f"  - Duplicates removed: {duplicates_removed}")
        
        # Write to Delta table
        logger.info(f"Writing to Delta table: {output_table}")
        silver_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
        
        logger.info("✅ Silver layer transformation complete")
        logger.info("=" * 70)
        
        return silver_df
        
    except Exception as e:
        logger.error(f"❌ Error in Silver layer transformation: {str(e)}")
        raise


def main():
    """Main execution function"""
    # Configuration
    INPUT_TABLE = "bronze_credit_transactions"
    OUTPUT_TABLE = "silver_credit_transactions"
    
    # Run transformation
    silver_df = run_silver_transformation(spark, INPUT_TABLE, OUTPUT_TABLE)
    
    # Display sample data
    silver_df.show(5)
    silver_df.printSchema()
    
    return silver_df


if __name__ == "__main__":
    main()
