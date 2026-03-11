# 🏦 Kavya Retail Banking Analytics Platform
### Snowflake + dbt | End-to-End Production Data Engineering Project

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

---

## 📌 Project Overview

A production-grade **Retail Banking Analytics Platform** built on **Snowflake** and **dbt**, modelled on a Monzo-style fintech use case. The project covers the full data engineering lifecycle  from environment setup and raw ingestion through to CDC pipelines, a star schema mart layer with incremental loading, data contracts, 82 automated tests, and a CI/CD deployment design.

**Domain:** Retail Banking  Fraud Detection & Customer Analytics  
**Dataset:** Kaggle Bank Transaction Dataset for Fraud Detection  
**Database:** Snowflake (KAVYA_DB)  
**Schemas:** KAVYA_RAW · KAVYA_STAGE · KAVYA_ANALYTICS

---

## 🏗️ End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE LAYER                          │
│                                                             │
│  KAVYA_RAW                                                  │
│  ├── 8 permanent tables (customers, accounts, transactions  │
│  │   fraud_alerts, products, branches, customer_products,   │
│  │   exchange_rates)                                        │
│  ├── Time Travel (7-day retention)                          │
│  ├── Zero Copy Clone (KAVYA_RAW_CLONE)                      │
│  └── CDC: 3 Streams + 3 Tasks (1-5 min schedule)           │
│                          │                                  │
│                          ▼                                  │
│  KAVYA_ANALYTICS (Snowflake Views)                          │
│  ├── VW_CUSTOMER_ACCOUNT_SUMMARY  (standard view)           │
│  ├── VW_SECURE_CUSTOMER_PII       (secure view - GDPR)      │
│  ├── MVW_DAILY_FRAUD_SUMMARY      (materialized view)       │
│  └── VW_TRANSACTION_ENRICHED      (FX enriched + risk)      │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      dbt LAYER                              │
│                                                             │
│  Staging (KAVYA_STAGE · view)                               │
│  └── 8 models: deduplicate · cast · filter · standardise   │
│                          │                                  │
│                          ▼                                  │
│  Intermediate (KAVYA_STAGE · table)                         │
│  └── 3 models: joins · FX enrichment · fraud aggregation   │
│                          │                                  │
│                          ▼                                  │
│  Marts (KAVYA_ANALYTICS · table / incremental)              │
│  ├── dim_customers  (table · model contract enforced)       │
│  ├── dim_accounts   (table · model contract enforced)       │
│  └── fct_transactions (incremental · 3-day late arrival)   │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
              BI Dashboards & Reporting
```

---

## 📁 Repository Structure

```
kavya_banking/
│
├── snowflake_scripts/                   # Pure Snowflake SQL (Phases 2–3)
│   ├── Phase2_Environment_Setup.sql     # DB, schemas, warehouses
│   ├── Phase3A_Raw_Tables.sql           # 8 raw tables + sample data
│   └── Phase3B_Advanced_Features.sql   # Time travel, clone, streams, tasks, views
│
├── models/                              # dbt transformation models (Phases 4–5)
│   ├── sources/
│   │   └── sources.yml                  # 8 source definitions + freshness checks
│   ├── staging/                         # 8 staging views + schema.yml
│   ├── intermediate/                    # 3 intermediate tables
│   └── marts/                           # 2 dims + 1 incremental fact + schema.yml
│
├── macros/
│   ├── generate_surrogate_key.sql       # MD5 hash surrogate key
│   ├── classify_risk.sql                # Fraud score → risk label
│   └── test_positive_values.sql         # Custom generic test
│
├── seeds/
│   ├── transaction_categories.csv       # Category group lookup
│   └── country_risk_ratings.csv         # Country fraud risk tier
│
├── tests/
│   ├── assert_no_zero_amount_transactions.sql
│   └── assert_fraud_score_range.sql
│
└── dbt_project.yml                      # Project config + materialization strategy
```

---

## 🔷 Phase 2 — Snowflake Environment Setup

```sql
-- Database with 7-day time travel
CREATE DATABASE KAVYA_DB DATA_RETENTION_TIME_IN_DAYS = 7;

-- 3 schemas with different retention policies
CREATE SCHEMA KAVYA_RAW       DATA_RETENTION_TIME_IN_DAYS = 7;
CREATE SCHEMA KAVYA_STAGE     DATA_RETENTION_TIME_IN_DAYS = 1;
CREATE SCHEMA KAVYA_ANALYTICS DATA_RETENTION_TIME_IN_DAYS = 1;

-- 3 purpose-built warehouses (auto-suspend/resume enabled)
CREATE WAREHOUSE KAVYA_LOAD_WH      WAREHOUSE_SIZE = 'SMALL';
CREATE WAREHOUSE KAVYA_TRANSFORM_WH WAREHOUSE_SIZE = 'SMALL';
CREATE WAREHOUSE KAVYA_ANALYTICS_WH WAREHOUSE_SIZE = 'X-SMALL';
```

---

## 🔷 Phase 3A — Raw Layer (8 Tables)

| Table | Type | Rows | Key Design |
|---|---|---|---|
| RAW_CUSTOMERS | Permanent | 10 | 7-day time travel, KYC and credit score |
| RAW_ACCOUNTS | Permanent | 12 | Clustered by ACCOUNT_TYPE + STATUS |
| RAW_TRANSACTIONS | Permanent | 16 | Clustered by DATE + ACCOUNT_ID |
| RAW_FRAUD_ALERTS | Permanent | 3 | Severity: LOW / MEDIUM / HIGH / CRITICAL |
| RAW_PRODUCTS | Permanent | 6 | Product catalogue with fees and rates |
| RAW_BRANCHES | Permanent | 10 | Physical and Digital branch types |
| RAW_CUSTOMER_PRODUCTS | Permanent | 10 | Customer-product subscriptions |
| RAW_EXCHANGE_RATES | **Transient** | 9 | GBP base · no fail-safe · 1-day retention |

---

## 🔷 Phase 3B — Advanced Snowflake Features

### ⏱️ Time Travel
```sql
SET TIME_TRAVEL_TS = CURRENT_TIMESTAMP();
UPDATE RAW_CUSTOMERS SET CREDIT_SCORE = 640 WHERE CUSTOMER_ID = 'CUST005';
DELETE FROM RAW_ACCOUNTS WHERE ACCOUNT_ID = 'ACC010';

-- Restore deleted row via time travel
INSERT INTO RAW_ACCOUNTS
SELECT * FROM RAW_ACCOUNTS AT (TIMESTAMP => $TIME_TRAVEL_TS)
WHERE ACCOUNT_ID = 'ACC010';
```

### 🔁 Zero Copy Clone
```sql
-- Clone entire schema instantly — no data copied, metadata only
CREATE SCHEMA KAVYA_DB.KAVYA_RAW_CLONE CLONE KAVYA_DB.KAVYA_RAW;
```

### 📡 Streams & Tasks (CDC Pipeline)
```
RAW_TRANSACTIONS  →  STM_TRANSACTIONS  →  TSK_PROCESS_TRANSACTIONS (every 1 min)
RAW_CUSTOMERS     →  STM_CUSTOMERS     →  TSK_PROCESS_CUSTOMERS    (every 5 min)
RAW_FRAUD_ALERTS  →  STM_FRAUD_ALERTS  →  TSK_PROCESS_FRAUD_ALERTS (every 1 min)
                                                ↓
                                   STG_*_CDC sink tables in KAVYA_STAGE
```

### 👁️ Views Strategy (4 Types)

| View | Type | Purpose |
|---|---|---|
| VW_CUSTOMER_ACCOUNT_SUMMARY | Standard | Customer + account + branch join |
| VW_SECURE_CUSTOMER_PII | **Secure** | PII masked — GDPR compliant |
| MVW_DAILY_FRAUD_SUMMARY | **Materialized** | Pre-aggregated fraud KPIs |
| VW_TRANSACTION_ENRICHED | Non-Materialized | FX enrichment + risk scoring |

---

## 🔶 Phase 4 — dbt Transformation Pipeline

### Staging Layer (view)
Lightweight renaming, casting, deduplication and null filtering. Zero storage cost  always reflects latest raw data.

### Intermediate Layer (table)

| Model | Logic |
|---|---|
| int_customer_accounts | 3-way join: customers + accounts + branches |
| int_transaction_enriched | FX enrichment + risk macro + customer context |
| int_fraud_summary | Daily fraud KPI aggregation per account |

### Mart Layer — Star Schema

| Model | Materialization | Contract |
|---|---|---|
| dim_customers | table | ✅ enforced |
| dim_accounts | table | ✅ enforced |
| fct_transactions | **incremental** | — |

### Incremental Strategy
```sql
{% if is_incremental() %}
    WHERE transaction_date >= (
        SELECT DATEADD('day', -3, MAX(transaction_date)) FROM {{ this }}
    )
{% endif %}
```
Processes only the last 3 days on each run — ~95% compute saving vs full refresh at scale.

---

## 🔶 Phase 5 — Testing, Macros & CI/CD

### 82 Tests · 100% Pass Rate
```
dbt test  →  PASS=82  WARN=0  ERROR=0
```

| Test Type | Count |
|---|---|
| unique + not_null | 44 |
| accepted_values (enums) | 18 |
| relationships (referential integrity) | 8 |
| Custom generic (positive_values) | 10 |
| Singular SQL tests | 2 |
| **TOTAL** | **82** |

### Macros
```sql
{{ generate_surrogate_key('customer_id') }}  -- MD5 surrogate key
{{ classify_risk('fraud_score') }}           -- LOW / MEDIUM / HIGH / CRITICAL
```

### CI/CD Pipeline Design (GitHub Actions)
```
PR opened
    └── dbt compile
    └── dbt test --select state:modified+
    └── dbt run --select state:modified+
    └── Block merge if any step fails

Merge to main
    └── dbt run --full-refresh
    └── dbt test (all 82 tests)
    └── dbt docs generate
    └── Slack notification
```

---

## 🚀 Getting Started

### Prerequisites
```bash
pip install dbt-snowflake
```

### Clone & Configure
```bash
git clone https://github.com/kavyarana/kavya-banking-dbt.git
cd kavya-banking-dbt
```

Create `~/.dbt/profiles.yml`:
```yaml
kavya_banking:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: <your_username>
      password: <your_password>
      role: ACCOUNTADMIN
      database: KAVYA_DB
      warehouse: KAVYA_TRANSFORM_WH
      schema: KAVYA_STAGE
      threads: 4
```

### Run the Full Pipeline
```bash
# 1. First run the Snowflake scripts in order:
#    snowflake_scripts/Phase2_Environment_Setup.sql
#    snowflake_scripts/Phase3A_Raw_Tables.sql
#    snowflake_scripts/Phase3B_Advanced_Features.sql

# 2. Then run dbt:
dbt debug          # Verify Snowflake connection
dbt seed           # Load 2 seed lookup tables
dbt run            # Run all 14 models
dbt test           # Run all 82 tests
dbt docs generate  # Build documentation site
dbt docs serve     # Open at http://localhost:8080
```

---

## 📊 Key Business KPIs Delivered

- Daily fraud transaction rate by country and channel
- Customer credit tier distribution (EXCELLENT / GOOD / FAIR / POOR)
- High-risk account flagging (fraud score >= 80 = CRITICAL)
- International vs domestic transaction split
- Product subscription revenue by customer segment
- Confirmed fraud amounts by account and date

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Snowflake** | Cloud data warehouse, raw storage, CDC pipelines, views |
| **dbt (v1.11)** | Transformations, testing, documentation, contracts |
| **SQL** | All transformation and analytical logic |
| **Python** | dbt runtime environment |
| **GitHub** | Version control and CI/CD pipeline design |
| **VS Code** | Development environment |

---

## 👩‍💻 Author

**Kavya Rana** — Data Architect & Lead Data Engineer  
9 years experience · Snowflake · dbt · AWS · PySpark · Azure  
[LinkedIn](https://linkedin.com/in/kavyarana) | [GitHub](https://github.com/kavyarana)
