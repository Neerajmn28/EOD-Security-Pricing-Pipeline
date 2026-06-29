# 📈 End-to-End EOD Securities Pricing Analytics Platform

> An end-to-end cloud data engineering project that automates the ingestion, transformation, validation, and visualization of U.S. End-of-Day (EOD) securities pricing data using Python, AWS, Apache Airflow, Snowflake, Docker, and Power BI.

---

## Project Overview

This project simulates a production-grade analytics platform for a global investment firm (**RBF**) that performs End-of-Day (EOD) market analysis to support trading, portfolio management, liquidity monitoring, and risk assessment.

The solution replaces manual CSV-based reporting with a fully automated batch pipeline that ingests market data, transforms it into a dimensional data warehouse, and exposes business-ready datasets for interactive Power BI dashboards.

---

## Business Problem

Financial analysts previously downloaded pricing files manually and prepared reports using spreadsheets.

This process resulted in:

- Delayed reporting
- Manual data preparation
- Increased operational risk
- Limited overnight market analysis
- Slower trading decisions

---

## Solution

The platform automates the complete daily analytics workflow:

- Extracts EOD market data from the Polygon API
- Uploads data to AWS S3
- Orchestrates workflows using Apache Airflow
- Loads data into Snowflake
- Performs data quality validation
- Builds a layered data warehouse
- Creates analytical views for reporting
- Delivers interactive Power BI dashboards
- Sends Slack notifications for pipeline monitoring

---

# Solution Architecture

```text
                    Polygon.io API
                           │
                           ▼
                 Python Data Extraction
                           │
                           ▼
                     Local CSV Files
                           │
                           ▼
                    Amazon S3 (Bronze)
                           │
                           ▼
                   Apache Airflow DAG
                           │
        ┌──────────────────┴──────────────────┐
        ▼                                     ▼
 Snowflake RAW Layer                 Data Quality Checks
        │
        ▼
 Snowflake CORE Layer
        │
        ▼
 Dimension Tables
        │
        ▼
 Fact Table
        │
        ▼
 Semantic Analytics Layer
        │
        ▼
 Power BI Dashboards
        │
        ▼
 Slack Notifications
```

---

# Technology Stack

| Category | Technology |
|----------|------------|
| Programming | Python |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker |
| Cloud Storage | AWS S3 |
| Cloud Data Warehouse | Snowflake |
| Data Source | Polygon.io API |
| Analytics | Power BI |
| Notifications | Slack |
| Version Control | Git & GitHub |

---

# Project Structure

```
EOD-Securities-Pricing-Analytics/
│
├── dags/                  # Airflow DAGs
├── lib/                   # Python helper modules
├── sql/                   # Snowflake SQL scripts
├── dashboards/            # Power BI files
├── docker/                # Docker configuration
├── docs/                  # Architecture diagrams
├── requirements.txt
└── README.md
```

---

# Data Pipeline

## 1. Historical Data Extraction

Python extracts historical U.S. stock market data from the Polygon.io API.

Features:

- API authentication
- Automatic weekend/holiday handling
- CSV generation
- Metadata tracking
- Historical backfill support

---

## 2. AWS S3 Ingestion

The extracted CSV files are uploaded automatically into an Amazon S3 bucket, which serves as the landing zone for downstream processing.

---

## 3. Workflow Orchestration

Apache Airflow orchestrates the complete batch pipeline.

Pipeline tasks include:

- Download EOD data
- Verify generated files
- Upload to S3
- Load Snowflake RAW tables
- Execute quality checks
- Transform data
- Populate dimension tables
- Load fact tables
- Publish metrics
- Send Slack notifications

---

## 4. Snowflake Data Warehouse

The warehouse follows a layered architecture.

```
RAW
   │
   ▼
CORE
   │
   ▼
DIMENSIONS
   │
   ▼
FACT
   │
   ▼
SEMANTIC ANALYTICS
```

### RAW

Stores source data exactly as received.

### CORE

Standardizes and validates pricing data.

### DIMENSIONS

- DIM_SECURITY
- DIM_DATE

### FACT

- FACT_DAILY_PRICE

### SEMANTIC ANALYTICS

Business-ready views optimized for Power BI reporting.

---

# Data Quality

The pipeline automatically performs:

- Duplicate detection
- Symbol standardization
- Incremental MERGE operations
- Freshness validation
- Record count verification
- Data completeness checks
- Reject record handling

Invalid records are redirected into reject tables without impacting analytical datasets.

---

# Semantic Analytics Layer

The Semantic Analytics (SA) layer exposes curated business views that simplify reporting and isolate business logic from the warehouse model.

Available views include:

| View | Description |
|------|-------------|
| VW_SECURITY_DAILY_PRICES | Daily pricing with company attributes |
| VW_TOP20_EQUITY_BY_VOLUME_DAILY | Top 20 equities by trading volume |
| VW_WATCHLIST_HISTORY | Historical performance of watchlist stocks |
| VW_SECURITY_LAST_30D_DAILY_RETURN | Rolling 30-day daily returns |
| VW_SECTOR_LIQUIDITY_LATEST | Sector liquidity contribution |
| VW_ETF_LIQUIDITY_30D_SUMMARY | ETF liquidity analysis |

---

# 📈 Power BI Dashboards

The dashboards provide business insights including:

- Equity Liquidity Analysis
- ETF Liquidity Analysis
- Watchlist Performance
- Daily Market Movers
- Sector Liquidity Contribution
- Trading Volume Intelligence
- Daily Return Analysis

Dashboards refresh automatically after successful pipeline execution.

---

# Monitoring

Operational monitoring includes:

- Airflow task monitoring
- Snowflake validation checks
- Slack notifications
- Pipeline failure alerts
- Load metrics

---

# Business Value

The solution delivers:

- Automated daily market reporting
- Reduced manual effort
- Improved data quality
- Faster trading insights
- Better sector liquidity visibility
- Scalable cloud architecture
- Reliable decision support

---

# Key Data Engineering Concepts

- ETL Pipeline Development
- Cloud Data Engineering
- Apache Airflow Orchestration
- Snowflake Data Warehousing
- Star Schema Design
- Incremental Data Loading
- Data Quality Validation
- Semantic Layer Design
- AWS S3 Integration
- Docker Containerization
- Power BI Reporting
- Operational Monitoring
