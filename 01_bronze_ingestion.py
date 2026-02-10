"""
Bronze Layer - Data Ingestion
Reads raw CSV data and loads into Delta Lake bronze table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_bronze_ingestion(spark, input_path, output_table="bronze_credit_transactions"):
    """
    Ingest raw CSV data into Bronze layer Delta table
    
    Args:
        spark: SparkSession object
        input_path: Path to input CSV file
        output_table: Name of output Delta table
    
    Returns:
        DataFrame: Bronze layer DataFrame
    """
    try:
        logger.info("=" * 70)
        logger.info("Starting Bronze Layer ETL - Data Ingestion")
        logger.info("=" * 70)
        
        # Read raw CSV data
        logger.info(f"Reading CSV from: {input_path}")
        bronze_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")  # Keep raw data types
            .csv(input_path)
        )
        
        # Rename first column to transaction_id
        bronze_df = bronze_df.withColumnRenamed(bronze_df.columns[0], "transaction_id")
        
        # Add metadata columns
        bronze_df = (
            bronze_df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        
        logger.info(f"Bronze data loaded: {bronze_df.count()} rows")
        logger.info(f"Schema: {bronze_df.columns}")
        
        # Write to Delta table
        logger.info(f"Writing to Delta table: {output_table}")
        bronze_df.write.format("delta").mode("overwrite").saveAsTable(output_table)
        
        logger.info("✅ Bronze layer ingestion complete")
        logger.info("=" * 70)
        
        return bronze_df
        
    except Exception as e:
        logger.error(f"❌ Error in Bronze layer ingestion: {str(e)}")
        raise


def main():
    """Main execution function"""
    # Initialize Spark session (for Databricks, spark is already available)
    # spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    
    # Configuration
    INPUT_PATH = "/Volumes/workspace/default/credit_data/credit_card_transactions.csv"
    OUTPUT_TABLE = "bronze_credit_transactions"
    
    # Run ingestion
    bronze_df = run_bronze_ingestion(spark, INPUT_PATH, OUTPUT_TABLE)
    
    # Display sample data
    bronze_df.show(5)
    
    return bronze_df


if __name__ == "__main__":
    main()
