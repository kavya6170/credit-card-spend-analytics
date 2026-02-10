"""
Bronze Layer - Data Ingestion
Reads raw CSV data and loads into Delta Lake bronze table
"""

from pyspark.sql.functions import current_timestamp, col
from pyspark import pipelines as dp

@dp.materialized_view()
def bronze_credit_transactions():
    # Read raw CSV data
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # Keep raw data types
        .csv("/Volumes/workspace/default/credit_data/credit_card_transactions.csv")
    )
    # Rename first column to transaction_id
    df = df.withColumnRenamed(df.columns[0], "transaction_id")
    # Add metadata columns
    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
    return df
