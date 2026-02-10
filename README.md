💳 Credit Card Spend Analytics & Recommendation Engine










🚀 Overview

A production-ready end-to-end Data Engineering & Analytics pipeline built using Databricks, PySpark, Delta Lake, and Power BI to process large-scale credit card transaction datasets, generate actionable business insights, and create personalized customer spending recommendations.

The solution implements the Medallion Architecture (Bronze → Silver → Gold) demonstrating scalable ETL pipeline design, data quality validation, and enterprise-grade BI analytics.

🎯 Business Objective

Financial institutions process millions of daily transactions and require real-time insights for analytics and marketing.

This project enables:

📊 Customer spending behavior analysis

🚨 Fraud trend monitoring

🏪 Merchant performance analytics

🎯 Category-based customer recommendations

📈 BI dashboard reporting for business teams

🏗 Architecture
Raw CSV Data
     ↓
Bronze Layer (Raw Ingestion)
     ↓
Silver Layer (Cleaning & Transformation)
     ↓
Gold Layer (Aggregated Business Metrics)
     ↓
Recommendation Engine
     ↓
Power BI Dashboard / JSON Exports

📂 Project Structure
credit-card-spend-analytics/
│
├── 01_bronze_ingestion.py
├── 02_silver_transformation.py
├── 03_gold_aggregation.py
├── 04_recommendation_engine.py
├── 05_export_pipeline.py
├── Dashboard.pbix
└── README.md

✨ Key Features

Medallion Architecture Implementation

Scalable PySpark ETL Pipelines

Delta Lake Transactional Storage

Customer Recommendation Engine

Fraud & Spending Pattern Analytics

Power BI Dashboard Integration

Production-style Modular Pipeline

Data Quality Validation & Logging

🧰 Technology Stack

Databricks

PySpark

Delta Lake

Python

SQL

Power BI

🔄 Data Pipeline Flow
Bronze Layer

Raw transaction CSV ingestion

Stores immutable raw data

Maintains ingestion metadata

Silver Layer

Deduplication

Null value handling

Schema standardization

Data validation

Gold Layer

Category spending analytics

Fraud detection trends

Merchant performance metrics

Daily transaction KPIs

Customer category recommendations

🤖 Recommendation Engine

Analyzes customer transaction patterns and generates top spending category recommendations to enable personalized marketing campaigns and targeted financial offers.

📊 Dashboard & Reporting

Power BI dashboards provide:

Category-wise spend insights

Fraud rate visualization

Daily transaction trends

Top merchant performance

Customer recommendation analytics

KPI summary metrics

▶️ How to Run (Databricks)

Execute scripts sequentially:

01_bronze_ingestion.py
02_silver_transformation.py
03_gold_aggregation.py
04_recommendation_engine.py
05_export_pipeline.py

💼 Industry Use Cases

Banking analytics platforms

FinTech recommendation engines

Fraud detection pipelines

Retail transaction intelligence

Enterprise data engineering portfolios

🔮 Future Enhancements

Real-time streaming ingestion using Kafka

ML-based fraud detection model

Automated Airflow / Databricks workflows

Customer segmentation using clustering

REST API for recommendation serving

👩‍💻 Author

Kavya Chougule
Big Data Analytics Engineer
PySpark | Databricks | Data Engineering | Analytics

📜 License

Created for educational and professional portfolio demonstration purposes.

📌 Version

v1.0.0
