Credit Card Spend Analytics & Recommendation Engine
Overview

A production-ready end-to-end data engineering and analytics pipeline built using Databricks, PySpark, Delta Lake, and Power BI to process credit card transaction data, generate business insights, and create personalized customer spending recommendations.

The project implements the Medallion Architecture (Bronze → Silver → Gold) and demonstrates scalable ETL design, data quality validation, and BI-ready analytics outputs.

Business Objective

Financial institutions generate massive volumes of transaction data daily.
This project enables:

Customer spending behavior analysis

Fraud trend monitoring

Merchant performance analytics

Category-based customer recommendations

BI dashboard reporting for business teams

Architecture
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

Project Structure
credit-card-spend-analytics/
│
├── 01_bronze_ingestion.py        # Raw data ingestion (Bronze layer)
├── 02_silver_transformation.py   # Data cleaning and transformation (Silver)
├── 03_gold_aggregation.py        # Business aggregations (Gold)
├── 04_recommendation_engine.py   # Customer recommendation logic
├── 05_export_pipeline.py         # Export to JSON / BI systems
├── Dashboard.pbix                # Power BI dashboard
└── README.md

Key Features

Medallion Architecture implementation

End-to-end ETL pipeline using PySpark

Delta Lake transactional storage

Customer recommendation engine

Fraud and spending analytics

Power BI dashboard integration

Production-style modular pipeline design

Data quality validation and logging

Technology Stack

Databricks

PySpark

Delta Lake

Python

SQL

Power BI

Data Pipeline Flow
Bronze Layer

Ingests raw transaction CSV data

Stores raw data without transformation

Maintains ingestion metadata

Silver Layer

Deduplication

Null value handling

Schema standardization

Data validation

Gold Layer

Category spending analytics

Fraud detection trends

Top merchant analysis

Daily transaction metrics

Customer category recommendations

KPI summary metrics

Recommendation Engine

The recommendation engine analyzes customer spending patterns and suggests top spending categories for personalized marketing campaigns and targeted offers.

Dashboard & Reporting

Power BI dashboards provide:

Category-wise spend insights

Fraud rate visualization

Daily transaction trends

Top merchant performance

Customer recommendation analytics

KPI summary metrics

How to Run (Databricks)

Run the scripts sequentially:

01_bronze_ingestion.py
02_silver_transformation.py
03_gold_aggregation.py
04_recommendation_engine.py
05_export_pipeline.py

Sample Use Cases

Banking analytics platforms

FinTech customer recommendation systems

Fraud analytics pipelines

Retail transaction intelligence platforms

Enterprise data engineering portfolio projects

Future Enhancements

Real-time streaming ingestion using Kafka

ML-based fraud detection model

Automated Airflow / Databricks workflow scheduling

Customer segmentation using clustering

REST API for recommendation serving

Author

Kavya Chougule
Big Data Analytics Engineer | Data Engineering | PySpark | Databricks

License

This project is created for educational and portfolio demonstration purposes.

Version

v1.0.0
