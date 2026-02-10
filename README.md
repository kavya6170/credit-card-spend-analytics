# Credit Card Analytics ETL Pipeline

## 📊 Overview

Production-ready ETL pipeline for credit card transaction analytics using Databricks, PySpark, and Delta Lake. Implements Medallion Architecture (Bronze-Silver-Gold) with Power BI integration.

## 🏗️ Architecture

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleaned & Transformed)
    ↓
Gold Layer (Business Analytics)
    ↓
Power BI / Dashboards
```

## 📁 Project Structure

```
credit-card-spend-analytics/
├── 01_bronze_ingestion.py          # Bronze layer: Raw data ingestion
├── 02_silver_transformation.py     # Silver layer: Data cleaning
├── 03_gold_aggregation.py          # Gold layer: Analytics aggregation
├── 04_recommendation_engine.py     # ML-based recommendations
├── 05_export_pipeline.py           # Power BI export & JSON generation
├── schema_definitions.sql          # DDL for all tables and views
├── Dashboard.lvdash.json           # Databricks SQL dashboard
└── README.md                       # This file
```

## 🚀 Quick Start

### Option 1: Run in Databricks Notebooks

Upload the Python scripts to Databricks workspace and run them sequentially:

```python
# 1. Bronze Layer
%run /Workspace/path/to/01_bronze_ingestion

# 2. Silver Layer
%run /Workspace/path/to/02_silver_transformation

# 3. Gold Layer
%run /Workspace/path/to/03_gold_aggregation

# 4. Recommendations
%run /Workspace/path/to/04_recommendation_engine

# 5. Export & Power BI
%run /Workspace/path/to/05_export_pipeline
```

### Option 2: Databricks Workflows (Recommended for Production)

Create a Databricks Workflow with 5 sequential tasks:

1. **Task 1**: Bronze Ingestion
   - Type: Python script
   - Source: `01_bronze_ingestion.py`

2. **Task 2**: Silver Transformation
   - Type: Python script
   - Source: `02_silver_transformation.py`
   - Depends on: Task 1

3. **Task 3**: Gold Aggregation
   - Type: Python script
   - Source: `03_gold_aggregation.py`
   - Depends on: Task 2

4. **Task 4**: Recommendation Engine
   - Type: Python script
   - Source: `04_recommendation_engine.py`
   - Depends on: Task 2

5. **Task 5**: Export Pipeline
   - Type: Python script
   - Source: `05_export_pipeline.py`
   - Depends on: Task 3, Task 4

### Option 3: Command Line Execution

```bash
# Using Databricks CLI
databricks workspace import 01_bronze_ingestion.py /path/to/workspace/
databricks jobs run-now --job-id <your-job-id>
```

## 📊 Data Flow

### Input
- **Source**: CSV file in Databricks Volume
- **Path**: `/Volumes/workspace/default/credit_data/credit_card_transactions.csv`

### Bronze Layer
- **Table**: `bronze_credit_transactions`
- **Purpose**: Raw data ingestion with metadata
- **Transformations**: None (preserve raw data)

### Silver Layer
- **Table**: `silver_credit_transactions`
- **Purpose**: Cleaned and standardized data
- **Transformations**:
  - Deduplication
  - Null handling
  - Type casting
  - Date parsing

### Gold Layer
- **Tables**:
  - `gold_spend_by_category` - Spending analysis by category
  - `gold_fraud_by_state` - Fraud rates by state
  - `gold_top_merchants` - Top 10 merchants by revenue
  - `gold_daily_transaction_trend` - Daily transaction trends
  - `gold_customer_category_recommendations` - Top 3 categories per customer
  - `gold_summary_metrics` - KPI metrics

### Power BI Views
- `powerbi_category_analytics`
- `powerbi_fraud_analytics`
- `powerbi_daily_trends`
- `powerbi_top_merchants`
- `powerbi_customer_recommendations`
- `powerbi_summary_metrics`

## 📈 Power BI Integration

### Connection Steps

1. **Get Databricks Connection Details**
   - Server Hostname: `<workspace>.cloud.databricks.com`
   - HTTP Path: Cluster → Advanced Options → JDBC/ODBC
   - Access Token: User Settings → Access Tokens

2. **Connect Power BI Desktop**
   - Get Data → Azure → Azure Databricks
   - Enter connection details
   - Use Personal Access Token for authentication

3. **Select Data**
   - Use Power BI optimized views (prefix: `powerbi_*`)
   - Choose DirectQuery or Import mode

4. **Create Visualizations**
   - KPI Cards: Revenue, Transactions, Fraud Rate
   - Charts: Category spend, Fraud by state, Daily trends
   - Tables: Top merchants, Recommendations

## 🛠️ Configuration

### Custom Configuration

```python
config = {
    "input_path": "/path/to/your/data.csv",
    "bronze_table": "bronze_credit_transactions",
    "silver_table": "silver_credit_transactions",
    "export_path": "dbfs:/FileStore/exports"
}

pipeline = CreditCardETLPipeline(spark, config)
pipeline.run_pipeline()
```

## 📊 Output Files

### Delta Tables
- 8 Delta tables (1 Bronze, 1 Silver, 6 Gold)

### JSON Exports
- Location: `dbfs:/FileStore/exports/`
- Files:
  - `customer_recommendations.json`
  - `category_analytics.json`
  - `fraud_analytics.json`
  - `daily_trends.json`
  - `top_merchants.json`
  - `summary_metrics.json`

### Dashboard
- `Dashboard.lvdash.json` - Import in Databricks SQL

## 🔍 Data Quality

### Built-in Checks
- ✅ Deduplication by transaction_id
- ✅ Null value handling
- ✅ Safe type casting with try_cast
- ✅ Fraud transaction filtering
- ✅ Row count validation

### Manual Checks

```sql
-- Check for duplicates
SELECT transaction_id, COUNT(*) as count
FROM bronze_credit_transactions
GROUP BY transaction_id
HAVING count > 1;

-- Check null values
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN merchant IS NULL THEN 1 ELSE 0 END) as null_merchants
FROM silver_credit_transactions;
```

## 📝 Logging

Each script includes comprehensive logging:
- INFO: Progress updates
- ERROR: Error messages with stack traces
- Log files: `etl_pipeline_YYYYMMDD_HHMMSS.log`

## 🔄 Scheduling with Databricks Workflows

### Create Workflow via UI

1. **Navigate to Workflows**
   - Go to Databricks workspace → Workflows → Create Job

2. **Add Tasks**
   - Task 1: Bronze Ingestion (`01_bronze_ingestion.py`)
   - Task 2: Silver Transformation (`02_silver_transformation.py`) - Depends on Task 1
   - Task 3: Gold Aggregation (`03_gold_aggregation.py`) - Depends on Task 2
   - Task 4: Recommendations (`04_recommendation_engine.py`) - Depends on Task 2
   - Task 5: Export (`05_export_pipeline.py`) - Depends on Task 3 & 4

3. **Configure Schedule**
   - Schedule: Daily at 2:00 AM
   - Timezone: Your timezone
   - Pause/Resume: As needed

4. **Set Alerts**
   - On failure: Email notification
   - On success: Optional

### Workflow DAG Structure

```
Bronze (Task 1)
    ↓
Silver (Task 2)
    ↓
    ├─→ Gold (Task 3) ──┐
    │                   ↓
    └─→ Recommendations (Task 4) ──→ Export (Task 5)
```

### Example Workflow JSON

```json
{
  "name": "Credit Card Analytics ETL",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "python_script_path": "/Workspace/path/to/01_bronze_ingestion.py"
    },
    {
      "task_key": "silver_transformation",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "python_script_path": "/Workspace/path/to/02_silver_transformation.py"
    },
    {
      "task_key": "gold_aggregation",
      "depends_on": [{"task_key": "silver_transformation"}],
      "python_script_path": "/Workspace/path/to/03_gold_aggregation.py"
    },
    {
      "task_key": "recommendations",
      "depends_on": [{"task_key": "silver_transformation"}],
      "python_script_path": "/Workspace/path/to/04_recommendation_engine.py"
    },
    {
      "task_key": "export",
      "depends_on": [
        {"task_key": "gold_aggregation"},
        {"task_key": "recommendations"}
      ],
      "python_script_path": "/Workspace/path/to/05_export_pipeline.py"
    }
  ],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

## 🎯 Key Features

- ✅ **Medallion Architecture** - Bronze → Silver → Gold
- ✅ **Delta Lake** - ACID transactions, time travel
- ✅ **Modular Design** - Reusable Python modules
- ✅ **Error Handling** - Comprehensive try-catch blocks
- ✅ **Logging** - Detailed execution logs
- ✅ **Power BI Ready** - Optimized views for BI
- ✅ **JSON Exports** - API-ready data
- ✅ **Data Quality** - Built-in validation

## 📚 Technologies

- **Databricks** - Unified analytics platform
- **PySpark** - Distributed data processing
- **Delta Lake** - ACID storage layer
- **Python** - ETL orchestration
- **SQL** - Schema definitions
- **Power BI** - Data visualization

## 🤝 Contributing

1. Follow PEP 8 style guide
2. Add logging to new functions
3. Include error handling
4. Update documentation

## 📄 License

This project is for educational and demonstration purposes.

---

**Last Updated**: 2026-02-10
**Version**: 1.0.0
