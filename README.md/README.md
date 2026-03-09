# 🏦 Kavya Retail Banking Analytics Platform
### Snowflake + dbt | End-to-End Production Data Engineering Project

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

---

## 📌 Project Overview

A production-grade **Retail Banking Analytics Platform** built on **Snowflake** and **dbt**, modelled on a Monzo-style fintech use case. The project covers the full data engineering lifecycle — from raw ingestion and CDC pipelines through to a star schema mart layer with incremental loading, data contracts, and a CI/CD deployment design.

**Domain:** Retail Banking — Fraud Detection & Customer Analytics  
**Dataset:** Kaggle Bank Transaction Dataset for Fraud Detection  
**Database:** Snowflake (KAVYA_DB)

---

## 🏗️ Architecture

```
Raw Layer (KAVYA_RAW)
    │   8 permanent tables · CDC Streams & Tasks · Time Travel · Zero Copy Clone
    ▼
Staging Layer (KAVYA_STAGE)  ← dbt views
    │   8 staging models · deduplication · type casting · null filtering
    ▼
Intermediate Layer (KAVYA_STAGE)  ← dbt tables
    │   int_customer_accounts · int_transaction_enriched · int_fraud_summary
    ▼
Mart Layer (KAVYA_ANALYTICS)  ← dbt tables + incremental
    │   dim_customers · dim_accounts · fct_transactions (incremental)
    ▼
Analytics & Reporting
    │   4 Snowflake views · dbt docs site · BI-ready star schema
```

---

## 📁 Project Structure

```
kavya_banking/
├── models/
│   ├── sources/          # Source definitions with freshness checks & tests
│   ├── staging/          # 8 staging views (one per raw table)
│   ├── intermediate/     # 3 intermediate tables (joins + aggregations)
│   └── marts/            # dim_customers, dim_accounts, fct_transactions
├── macros/
│   ├── generate_surrogate_key.sql   # MD5 surrogate key macro
│   ├── classify_risk.sql            # Fraud score → risk label macro
│   └── test_positive_values.sql     # Custom generic test
├── seeds/
│   ├── transaction_categories.csv   # Category lookup table
│   └── country_risk_ratings.csv     # Country risk tier lookup
├── tests/
│   ├── assert_no_zero_amount_transactions.sql
│   └── assert_fraud_score_range.sql
└── dbt_project.yml
```

---

## ⚙️ Snowflake Features Demonstrated

| Feature | Implementation |
|---|---|
| **Time Travel** | 7-day retention on KAVYA_RAW · historical query · deleted row restore |
| **Zero Copy Clone** | Full schema clone (KAVYA_RAW_CLONE) · independent mutation test |
| **Streams** | STM_TRANSACTIONS, STM_CUSTOMERS, STM_FRAUD_ALERTS on raw tables |
| **Tasks** | CDC pipeline tasks with SYSTEM$STREAM_HAS_DATA() condition |
| **Materialized View** | MVW_DAILY_FRAUD_SUMMARY — pre-aggregated fraud KPIs |
| **Secure View** | VW_SECURE_CUSTOMER_PII — masked email, phone, DOB for GDPR |
| **Transient Table** | RAW_EXCHANGE_RATES — no fail-safe, 1-day retention |
| **Clustering** | RAW_TRANSACTIONS clustered by TRANSACTION_DATE + ACCOUNT_ID |

---

## 🔁 dbt Models

### Staging (view)
| Model | Source | Key Transformations |
|---|---|---|
| stg_customers | RAW_CUSTOMERS | Deduplicate, trim names, UPPER enums |
| stg_accounts | RAW_ACCOUNTS | Deduplicate, cast balances to FLOAT |
| stg_transactions | RAW_TRANSACTIONS | Deduplicate, filter amount > 0 |
| stg_fraud_alerts | RAW_FRAUD_ALERTS | Deduplicate, standardise severity |
| stg_products | RAW_PRODUCTS | Deduplicate, cast fees and rates |
| stg_branches | RAW_BRANCHES | Deduplicate, UPPER postcode |
| stg_customer_products | RAW_CUSTOMER_PRODUCTS | Deduplicate subscriptions |
| stg_exchange_rates | RAW_EXCHANGE_RATES | Deduplicate by date + currency pair |

### Intermediate (table)
| Model | Purpose |
|---|---|
| int_customer_accounts | 3-way join: customers + accounts + branches |
| int_transaction_enriched | Transactions enriched with FX rates, risk classification, customer context |
| int_fraud_summary | Daily fraud KPI aggregation per account |

### Marts (table / incremental)
| Model | Type | Grain |
|---|---|---|
| dim_customers | table | One row per customer with credit tier |
| dim_accounts | table | One row per account with branch + product context |
| fct_transactions | **incremental** | One row per transaction — 3-day late arrival window |

---

## 🧪 Testing

**82 tests · 100% pass rate**

| Test Type | Count | Coverage |
|---|---|---|
| Built-in (unique, not_null, accepted_values, relationships) | 80 | All 8 sources + all models |
| Custom generic (test_positive_values) | 2 | amount, fraud_score columns |
| Singular SQL tests | 2 | Zero amounts, fraud score range |

---

## 🧩 Macros

```sql
-- Surrogate key generation
{{ generate_surrogate_key('customer_id') }}  →  MD5(CAST(customer_id AS VARCHAR))

-- Risk classification from fraud score
{{ classify_risk('fraud_score') }}  →  CASE WHEN score >= 80 THEN 'CRITICAL' ...
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- dbt-snowflake: `pip install dbt-snowflake`
- Snowflake account with KAVYA_DB database and KAVYA_RAW schema populated

### Setup

1. Clone the repo:
```bash
git clone https://github.com/kavyarana/kavya-banking-dbt.git
cd kavya-banking-dbt
```

2. Create `~/.dbt/profiles.yml`:
```yaml
kavya_banking:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_user>
      password: <your_password>
      role: ACCOUNTADMIN
      database: KAVYA_DB
      warehouse: KAVYA_TRANSFORM_WH
      schema: KAVYA_STAGE
      threads: 4
```

3. Run the pipeline:
```bash
dbt debug          # Test connection
dbt seed           # Load seed files
dbt run            # Run all models
dbt test           # Run all 82 tests
dbt docs generate  # Build docs site
dbt docs serve     # Open at http://localhost:8080
```

---

## 📋 CI/CD Design

The project includes a full CI/CD pipeline design using **GitHub Actions**:

- **On Pull Request:** `dbt compile` → `dbt test --select state:modified+` → `dbt run --select state:modified+`
- **On Merge to Main:** `dbt run --full-refresh` → `dbt test` → `dbt docs generate` → Slack notification

Credentials managed via GitHub Actions secrets — `profiles.yml` is never committed to the repository.

---

## 📊 Key Business KPIs

- Daily fraud transaction rate by country and channel
- Customer credit tier distribution and balance analysis
- High-risk account identification (fraud score > 80)
- International vs domestic transaction split
- Product subscription revenue by customer segment

---

## 👩‍💻 Author

**Kavya Rana** — Data Architect & Lead Data Engineer  
[LinkedIn](https://linkedin.com/in/kavyarana) | [GitHub](https://github.com/kavyarana)
