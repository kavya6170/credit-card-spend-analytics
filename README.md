# Credit Card Spend Analytics & Recommendation Engine

This repository contains an end-to-end **credit card analytics pipeline** built in **Databricks** using a Medallion Architecture (Bronze → Silver → Gold).  
It performs data ingestion, transformation, business aggregations, exports for APIs, and includes a recommendation engine.

A Databricks dashboard is also included in this repo.

---

## 📂 Repository Structure

01_Bronze_Data_Ingestion.ipynb
02_Silver_Data_Transformation.ipynb
03_Gold_Analytics_Aggregation.ipynb
04_Recommendation_Engine.ipynb
05_Export_Data_for_API.ipynb
Dashboard.lvdash.json
README.md


---

## 📌 Notebook Description

### 🧱 ETL Pipeline

| Notebook | Purpose |
|----------|---------|
| **01_Bronze_Data_Ingestion.ipynb** | Ingests raw credit card transaction data |
| **02_Silver_Data_Transformation.ipynb** | Cleans, standardizes, and prepares data |
| **03_Gold_Analytics_Aggregation.ipynb** | Creates business-ready Gold Delta tables |

### 📊 Analytics & Intelligence

| Notebook | Purpose |
|----------|---------|
| **04_Recommendation_Engine.ipynb** | Generates customer/merchant recommendations |
| **05_Export_Data_for_API.ipynb** | Exports analytics results for external systems |

---

## 🏗 Architecture

Raw CSV
↓
Bronze Layer (Databricks Delta)
↓
Silver Layer (Cleaned & Transformed)
↓
Gold Layer (Aggregated Business Tables)
↓
Databricks SQL Dashboard & API Exports


---

## 🧪 Gold Delta Tables Created

| Table | Description |
|-------|-------------|
| `gold_spend_by_category` | Total spend and transaction count by category |
| `gold_fraud_by_state` | Fraud statistics by state |
| `gold_top_merchants` | Top merchants by revenue |
| `gold_daily_transaction_trend` | Daily transaction and amount trends |

---

## 📊 Dashboard

This project includes a Databricks SQL dashboard configuration.

### 🔗 View the dashboard file here:
👉 https://github.com/kavya6170/credit-card-spend-analytics.git

Dashboard file:
Dashboard.lvdash.json


You can import this into **Databricks SQL → Dashboards → Import** to view the interactive dashboard.

---

## ▶ How to Run

1. Clone the repo:
```bash
git clone https://github.com/kavya6170/credit-card-spend-analytics.git
Open it in Databricks Repos

Run notebooks in order:

01_Bronze_Data_Ingestion
02_Silver_Data_Transformation
03_Gold_Analytics_Aggregation
04_Recommendation_Engine
05_Export_Data_for_API
Import the dashboard file in Databricks SQL.

🛠 Tools Used
Databricks

Apache Spark (PySpark)

Delta Lake

Databricks SQL

GitHub

🚀 Future Enhancements
Add BI tool dashboards (Power BI / Tableau)

Add Snowflake integration

Add ML-based fraud detection model

📜 License
MIT License


---

### Now commit it:

```bash
git add README.md
git commit -m "Add project README"
git push origin main
