# Supply Chain & Inventory Intelligence Platform

## Project Overview
An end-to-end data engineering project designed to process supply chain and inventory data, transform raw order data, and generate business insights using data engineering tools.

## Architecture
Python Producer → Apache Kafka → Python Consumer → Raw JSON → PySpark → Parquet → PostgreSQL → SQL Analytics → Power BI

## Tech Stack
- Python
- Apache Kafka
- PySpark
- PostgreSQL
- SQL
- Docker
- Power BI
- Git & GitHub

## Project Workflow

### 1. Data Generation
Python generates simulated supply chain order data.

### 2. Data Ingestion
Apache Kafka is used to stream order events.

### 3. Data Collection
The Kafka consumer collects incoming events and stores them in JSON format.

### 4. Data Processing
PySpark performs:
- Data cleaning
- Data validation
- Duplicate removal
- Data transformation
- Date extraction

### 5. Data Storage
Processed data is stored in Parquet format.

### 6. Database Loading
Processed records are loaded into PostgreSQL for analysis.

### 7. Business Analytics
SQL queries are used to analyze:
- Total orders
- Revenue
- Product performance
- Daily sales
- Monthly sales
- Order status

## Project Structure

```text
SupplyChain_Intelligence/
│
├── ingestion/
│   ├── kafka_producer.py
│   └── kafka_consumer.py
│
├── processing/
│   ├── spark_batch.py
│   └── load_to_postgres.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── sql/
│
├── dashboard/
│
├── docker-compose.yml
├── Dockerfile.spark
├── requirements.txt
└── README.md