-- ============================================================
--  KAVYA RETAIL BANKING ANALYTICS PLATFORM
--  PHASE 3B — Advanced Snowflake Features (Steps 4 & 5)
--  Author : Kavya Rana
-- ============================================================
--
--  WHAT WE COVER IN THIS SCRIPT:
--  ─────────────────────────────────────────────────────────
--  STEP 4A : Time Travel        → Update/Delete data, query history, restore
--  STEP 4B : Zero Copy Cloning  → Clone schema, validate, test independence
--  STEP 4C : Streams & Tasks    → CDC pipeline on 3 raw tables
--  STEP 5  : Views Strategy     → Standard, Secure, Materialized, Non-Materialized
-- ============================================================

USE ROLE      ACCOUNTADMIN;
USE WAREHOUSE KAVYA_LOAD_WH;
USE DATABASE  KAVYA_DB;
USE SCHEMA    KAVYA_DB.KAVYA_RAW;


-- ============================================================
-- STEP 4A — TIME TRAVEL
-- ============================================================
-- What is Time Travel?
-- Snowflake automatically keeps a history of all changes made
-- to a table. With Time Travel you can query, clone or restore
-- data as it existed at any point within the retention window.
-- We set DATA_RETENTION_TIME_IN_DAYS = 7 on all raw tables.
-- ============================================================

-- ── 4A.1 Record a timestamp BEFORE we make any changes ───────
SET TIME_TRAVEL_TS = CURRENT_TIMESTAMP();
SELECT $TIME_TRAVEL_TS AS snapshot_before_changes;

-- ── 4A.2 Check current state before changes ──────────────────
SELECT CUSTOMER_ID, FIRST_NAME, CREDIT_SCORE, KYC_STATUS
FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
WHERE CUSTOMER_ID = 'CUST005';
-- Expected: CREDIT_SCORE = 590, KYC_STATUS = 'PENDING'

SELECT COUNT(*) AS total_accounts
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS;
-- Expected: 12 rows

-- ── 4A.3 Make changes to simulate real-world updates ─────────

-- UPDATE: Amelia Wilson passed KYC verification and credit score improved
UPDATE KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
SET
    CREDIT_SCORE = 640,
    KYC_STATUS   = 'VERIFIED'
WHERE CUSTOMER_ID = 'CUST005';

-- DELETE: George Evans frozen account has been closed
DELETE FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
WHERE ACCOUNT_ID = 'ACC010';

-- ── 4A.4 Verify the changes took effect ──────────────────────
SELECT CUSTOMER_ID, FIRST_NAME, CREDIT_SCORE, KYC_STATUS
FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
WHERE CUSTOMER_ID = 'CUST005';
-- Expected: CREDIT_SCORE = 640, KYC_STATUS = 'VERIFIED'

SELECT COUNT(*) AS total_accounts
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS;
-- Expected: 11 rows (ACC010 deleted)

-- ── 4A.5 TIME TRAVEL: Query data AS IT WAS before changes ────

-- See CUST005 before the update using our saved timestamp
SELECT CUSTOMER_ID, FIRST_NAME, CREDIT_SCORE, KYC_STATUS
FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
    AT (TIMESTAMP => $TIME_TRAVEL_TS)
WHERE CUSTOMER_ID = 'CUST005';
-- Expected: CREDIT_SCORE = 590, KYC_STATUS = 'PENDING'

-- See the deleted account ACC010 using our saved timestamp
SELECT ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS, CURRENT_BALANCE
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
    AT (TIMESTAMP => $TIME_TRAVEL_TS)
WHERE ACCOUNT_ID = 'ACC010';
-- Expected: Returns the row that was deleted

-- ── 4A.6 TIME TRAVEL: Restore the deleted row ────────────────
-- If we need to recover the deleted account we restore it from history

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
SELECT *
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
    AT (TIMESTAMP => $TIME_TRAVEL_TS)
WHERE ACCOUNT_ID = 'ACC010';

-- Verify restore was successful
SELECT COUNT(*) AS total_accounts_after_restore
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS;
-- Expected: 12 rows again

SELECT ACCOUNT_ID, ACCOUNT_STATUS
FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
WHERE ACCOUNT_ID = 'ACC010';
-- Expected: Row is back

SELECT 'Time Travel demo complete' AS status;


-- ============================================================
-- STEP 4B — ZERO COPY CLONING
-- ============================================================
-- What is Zero Copy Cloning?
-- Creates an instant copy of a database, schema or table
-- WITHOUT duplicating the underlying data.
-- Both the source and clone share the same micro-partitions.
-- Storage only increases when one of them makes changes.
-- Use cases: UAT environments, developer sandboxes, backups.
-- ============================================================

-- ── 4B.1 Clone the entire KAVYA_RAW schema ───────────────────
-- This is instant regardless of how much data is in the schema
CREATE OR REPLACE SCHEMA KAVYA_DB.KAVYA_RAW_CLONE
    CLONE KAVYA_DB.KAVYA_RAW
    COMMENT = 'Zero copy clone of KAVYA_RAW — UAT testing environment, shares storage with source';

-- ── 4B.2 Validate the clone has identical data ───────────────
SELECT 'SOURCE' AS origin, COUNT(*) AS row_count FROM KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS
UNION ALL
SELECT 'CLONE',             COUNT(*) FROM KAVYA_DB.KAVYA_RAW_CLONE.RAW_TRANSACTIONS;
-- Expected: Both show the same count

SELECT 'SOURCE' AS origin, COUNT(*) AS row_count FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
UNION ALL
SELECT 'CLONE',             COUNT(*) FROM KAVYA_DB.KAVYA_RAW_CLONE.RAW_CUSTOMERS;
-- Expected: Both show the same count

-- ── 4B.3 Make a change in the CLONE only ─────────────────────
-- This proves the clone is fully independent from the source
UPDATE KAVYA_DB.KAVYA_RAW_CLONE.RAW_TRANSACTIONS
SET STATUS = 'REVERSED'
WHERE TRANSACTION_ID = 'TXN015';

-- ── 4B.4 Prove the SOURCE is completely unaffected ───────────
SELECT TRANSACTION_ID, STATUS
FROM KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS
WHERE TRANSACTION_ID = 'TXN015';
-- Expected: STATUS = 'PENDING' (unchanged in source)

SELECT TRANSACTION_ID, STATUS
FROM KAVYA_DB.KAVYA_RAW_CLONE.RAW_TRANSACTIONS
WHERE TRANSACTION_ID = 'TXN015';
-- Expected: STATUS = 'REVERSED' (changed only in clone)

SELECT 'Zero Copy Clone demo complete' AS status;


-- ============================================================
-- STEP 4C — STREAMS & TASKS (CDC Pipeline)
-- ============================================================
-- What are Streams?
-- A Stream is a change tracking object that records every
-- INSERT, UPDATE and DELETE made to a table (CDC).
-- It stores: METADATA$ACTION (INSERT/DELETE), METADATA$ISUPDATE
--
-- What are Tasks?
-- A Task is a scheduled job that automatically processes the
-- changes captured in a stream and writes them to a target table.
--
-- Our CDC Pipeline:
-- RAW table → Stream (captures changes) → Task (processes) → STAGE table
-- ============================================================

USE SCHEMA KAVYA_DB.KAVYA_RAW;

-- ── 4C.1 Create CDC target tables in KAVYA_STAGE ─────────────
-- These tables receive the processed records from streams via tasks

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_STAGE.STG_TRANSACTIONS_CDC (
    TRANSACTION_ID          VARCHAR(36),
    ACCOUNT_ID              VARCHAR(20),
    TRANSACTION_DATE        DATE,
    TRANSACTION_TIMESTAMP   TIMESTAMP_NTZ,
    TRANSACTION_TYPE        VARCHAR(50),
    TRANSACTION_CATEGORY    VARCHAR(100),
    MERCHANT_NAME           VARCHAR(255),
    AMOUNT                  NUMBER(18,2),
    CURRENCY_CODE           VARCHAR(3),
    CHANNEL                 VARCHAR(50),
    LOCATION_COUNTRY        VARCHAR(100),
    IS_FRAUD_FLAGGED        BOOLEAN,
    FRAUD_SCORE             NUMBER(5,2),
    STATUS                  VARCHAR(20),
    CDC_ACTION              VARCHAR(10),     -- INSERT or DELETE
    CDC_PROCESSED_AT        TIMESTAMP_NTZ
)
COMMENT = 'CDC sink for transactions — receives changes captured by STM_TRANSACTIONS via task';

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_STAGE.STG_CUSTOMERS_CDC (
    CUSTOMER_ID             VARCHAR(20),
    FIRST_NAME              VARCHAR(100),
    LAST_NAME               VARCHAR(100),
    CUSTOMER_SEGMENT        VARCHAR(50),
    KYC_STATUS              VARCHAR(20),
    CREDIT_SCORE            NUMBER(5,0),
    IS_ACTIVE               BOOLEAN,
    CDC_ACTION              VARCHAR(10),
    CDC_PROCESSED_AT        TIMESTAMP_NTZ
)
COMMENT = 'CDC sink for customers — receives changes captured by STM_CUSTOMERS via task';

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_STAGE.STG_FRAUD_ALERTS_CDC (
    ALERT_ID                VARCHAR(36),
    TRANSACTION_ID          VARCHAR(36),
    ALERT_TYPE              VARCHAR(100),
    ALERT_SEVERITY          VARCHAR(20),
    FRAUD_SCORE             NUMBER(5,2),
    IS_CONFIRMED_FRAUD      BOOLEAN,
    INVESTIGATION_STATUS    VARCHAR(50),
    CDC_ACTION              VARCHAR(10),
    CDC_PROCESSED_AT        TIMESTAMP_NTZ
)
COMMENT = 'CDC sink for fraud alerts — receives changes captured by STM_FRAUD_ALERTS via task';

-- ── 4C.2 Create Streams on 3 raw tables ──────────────────────

-- Stream 1: Transactions (highest velocity — most critical for CDC)
CREATE OR REPLACE STREAM KAVYA_DB.KAVYA_RAW.STM_TRANSACTIONS
    ON TABLE KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS
    COMMENT = 'CDC stream on RAW_TRANSACTIONS — captures all INSERT, UPDATE, DELETE changes';

-- Stream 2: Customers (tracks KYC, segment and credit score changes)
CREATE OR REPLACE STREAM KAVYA_DB.KAVYA_RAW.STM_CUSTOMERS
    ON TABLE KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
    COMMENT = 'CDC stream on RAW_CUSTOMERS — captures customer profile changes';

-- Stream 3: Fraud Alerts (real-time fraud investigation tracking)
CREATE OR REPLACE STREAM KAVYA_DB.KAVYA_RAW.STM_FRAUD_ALERTS
    ON TABLE KAVYA_DB.KAVYA_RAW.RAW_FRAUD_ALERTS
    COMMENT = 'CDC stream on RAW_FRAUD_ALERTS — feeds real-time fraud processing pipeline';

-- ── 4C.3 Create Tasks to process each stream ─────────────────

-- Task 1: Process new/changed transactions every 1 minute
CREATE OR REPLACE TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_TRANSACTIONS
    WAREHOUSE = KAVYA_TRANSFORM_WH
    SCHEDULE  = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_TRANSACTIONS')
AS
INSERT INTO KAVYA_DB.KAVYA_STAGE.STG_TRANSACTIONS_CDC
SELECT
    TRANSACTION_ID,
    ACCOUNT_ID,
    TRANSACTION_DATE,
    TRANSACTION_TIMESTAMP,
    TRANSACTION_TYPE,
    TRANSACTION_CATEGORY,
    MERCHANT_NAME,
    AMOUNT,
    CURRENCY_CODE,
    CHANNEL,
    LOCATION_COUNTRY,
    IS_FRAUD_FLAGGED,
    FRAUD_SCORE,
    STATUS,
    METADATA$ACTION      AS CDC_ACTION,
    CURRENT_TIMESTAMP()  AS CDC_PROCESSED_AT
FROM KAVYA_DB.KAVYA_RAW.STM_TRANSACTIONS
WHERE METADATA$ISUPDATE = FALSE OR METADATA$ACTION = 'INSERT';

-- Task 2: Process customer profile changes every 5 minutes
CREATE OR REPLACE TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_CUSTOMERS
    WAREHOUSE = KAVYA_TRANSFORM_WH
    SCHEDULE  = '5 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_CUSTOMERS')
AS
INSERT INTO KAVYA_DB.KAVYA_STAGE.STG_CUSTOMERS_CDC
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    CUSTOMER_SEGMENT,
    KYC_STATUS,
    CREDIT_SCORE,
    IS_ACTIVE,
    METADATA$ACTION      AS CDC_ACTION,
    CURRENT_TIMESTAMP()  AS CDC_PROCESSED_AT
FROM KAVYA_DB.KAVYA_RAW.STM_CUSTOMERS
WHERE METADATA$ISUPDATE = FALSE OR METADATA$ACTION = 'INSERT';

-- Task 3: Process fraud alert changes every 1 minute
CREATE OR REPLACE TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_FRAUD_ALERTS
    WAREHOUSE = KAVYA_TRANSFORM_WH
    SCHEDULE  = '1 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_FRAUD_ALERTS')
AS
INSERT INTO KAVYA_DB.KAVYA_STAGE.STG_FRAUD_ALERTS_CDC
SELECT
    ALERT_ID,
    TRANSACTION_ID,
    ALERT_TYPE,
    ALERT_SEVERITY,
    FRAUD_SCORE,
    IS_CONFIRMED_FRAUD,
    INVESTIGATION_STATUS,
    METADATA$ACTION      AS CDC_ACTION,
    CURRENT_TIMESTAMP()  AS CDC_PROCESSED_AT
FROM KAVYA_DB.KAVYA_RAW.STM_FRAUD_ALERTS
WHERE METADATA$ISUPDATE = FALSE OR METADATA$ACTION = 'INSERT';

-- ── 4C.4 Resume all tasks (tasks start SUSPENDED by default) ─
ALTER TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_TRANSACTIONS  RESUME;
ALTER TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_CUSTOMERS     RESUME;
ALTER TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_FRAUD_ALERTS  RESUME;

-- ── 4C.5 Trigger CDC by inserting new data ───────────────────
-- Insert a new transaction — the stream will capture this change

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS
    (TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_TIMESTAMP,
     TRANSACTION_TYPE, TRANSACTION_CATEGORY, MERCHANT_NAME, MERCHANT_CATEGORY_CODE,
     AMOUNT, CURRENCY_CODE, CHANNEL, LOCATION_CITY, LOCATION_COUNTRY,
     IS_FRAUD_FLAGGED, FRAUD_SCORE, REFERENCE_NUMBER, STATUS,
     _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('TXN017','ACC002','2024-03-10','2024-03-10 09:00:00',
 'DEBIT','GROCERIES','Marks and Spencer','5411',
 55.20,'GBP','CONTACTLESS','London','United Kingdom',
 FALSE,1.5,'REF017','COMPLETED','2024-03-10_txn.csv',CURRENT_TIMESTAMP());

-- Also update a customer to trigger STM_CUSTOMERS
UPDATE KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
SET CREDIT_SCORE = 660
WHERE CUSTOMER_ID = 'CUST002';

-- ── 4C.6 Check the streams have captured the changes ─────────
SELECT METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update,
       TRANSACTION_ID, AMOUNT, STATUS
FROM KAVYA_DB.KAVYA_RAW.STM_TRANSACTIONS;
-- Expected: Shows TXN017 with METADATA$ACTION = 'INSERT'

SELECT METADATA$ACTION AS action, METADATA$ISUPDATE AS is_update,
       CUSTOMER_ID, CREDIT_SCORE
FROM KAVYA_DB.KAVYA_RAW.STM_CUSTOMERS;
-- Expected: Shows CUST002 with two rows (DELETE old, INSERT new)

-- ── 4C.7 Check if streams have data ──────────────────────────
SELECT
    SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_TRANSACTIONS')  AS txn_stream_has_data,
    SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_CUSTOMERS')     AS cust_stream_has_data,
    SYSTEM$STREAM_HAS_DATA('KAVYA_DB.KAVYA_RAW.STM_FRAUD_ALERTS')  AS fraud_stream_has_data;

-- ── 4C.8 Manually execute tasks to process immediately ───────
EXECUTE TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_TRANSACTIONS;
EXECUTE TASK KAVYA_DB.KAVYA_RAW.TSK_PROCESS_CUSTOMERS;

-- ── 4C.9 Verify CDC tables received the records ──────────────
SELECT * FROM KAVYA_DB.KAVYA_STAGE.STG_TRANSACTIONS_CDC
ORDER BY CDC_PROCESSED_AT DESC;

SELECT * FROM KAVYA_DB.KAVYA_STAGE.STG_CUSTOMERS_CDC
ORDER BY CDC_PROCESSED_AT DESC;

SELECT 'Streams and Tasks CDC demo complete' AS status;


-- ============================================================
-- STEP 5 — VIEWS STRATEGY
-- 4 View Types: Standard, Secure, Materialized, Non-Materialized
-- ============================================================

USE WAREHOUSE KAVYA_ANALYTICS_WH;
USE SCHEMA    KAVYA_DB.KAVYA_ANALYTICS;

-- ── 5.1 STANDARD VIEW ────────────────────────────────────────
-- Purpose     : Easy joined view of customers and accounts
-- Storage     : NONE — virtual, recomputed on every query
-- Performance : Medium — no caching, always fresh
-- Use case    : Ad-hoc joins that are queried infrequently
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW KAVYA_DB.KAVYA_ANALYTICS.VW_CUSTOMER_ACCOUNT_SUMMARY
COMMENT = 'Standard view: joins customers + accounts + branches. No storage cost. Recomputed on every query.'
AS
SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME     AS FULL_NAME,
    c.CUSTOMER_SEGMENT,
    c.KYC_STATUS,
    c.CREDIT_SCORE,
    a.ACCOUNT_ID,
    a.ACCOUNT_TYPE,
    a.ACCOUNT_STATUS,
    a.CURRENT_BALANCE,
    a.CURRENCY_CODE,
    a.OPEN_DATE                             AS ACCOUNT_OPEN_DATE,
    b.BRANCH_NAME,
    b.CITY                                  AS BRANCH_CITY
FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS  c
JOIN KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS   a ON c.CUSTOMER_ID = a.CUSTOMER_ID
JOIN KAVYA_DB.KAVYA_RAW.RAW_BRANCHES   b ON a.BRANCH_ID   = b.BRANCH_ID;

-- Test standard view
SELECT * FROM KAVYA_DB.KAVYA_ANALYTICS.VW_CUSTOMER_ACCOUNT_SUMMARY
ORDER BY CUSTOMER_ID;


-- ── 5.2 SECURE VIEW ──────────────────────────────────────────
-- Purpose     : GDPR-compliant PII masking for analyst queries
-- Storage     : NONE — virtual like standard view
-- Performance : Slightly slower (query optimiser is bypassed
--               to prevent data inference attacks)
-- Security    : View DEFINITION is hidden from non-owners
--               Snowflake prevents query plan-based data leaks
-- Use case    : Exposing customer data to analysts without PII
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE SECURE VIEW KAVYA_DB.KAVYA_ANALYTICS.VW_SECURE_CUSTOMER_PII
COMMENT = 'Secure view: masks all PII (email, phone, DOB). Definition hidden from non-owners. GDPR compliant.'
AS
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    -- Mask email: show only first 3 chars
    LEFT(EMAIL, 3) || '****@****.com'           AS EMAIL_MASKED,
    -- Mask phone completely
    '07*** *** ***'                              AS PHONE_MASKED,
    -- Show only birth year, not full DOB
    YEAR(DATE_OF_BIRTH)                          AS BIRTH_YEAR,
    CITY,
    COUNTRY,
    CUSTOMER_SEGMENT,
    KYC_STATUS,
    CREDIT_SCORE,
    IS_ACTIVE
FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS;

-- Test secure view — confirm PII is masked
SELECT * FROM KAVYA_DB.KAVYA_ANALYTICS.VW_SECURE_CUSTOMER_PII
ORDER BY CUSTOMER_ID;


-- ── 5.3 MATERIALIZED VIEW ────────────────────────────────────
-- Purpose     : Pre-aggregated fraud KPIs for dashboard queries
-- Storage     : YES — physically stores result set on disk
-- Performance : FASTEST — result served from pre-computed store
--               Snowflake auto-refreshes it in the background
-- Cost        : Additional storage + background compute refresh
-- Use case    : Dashboard KPIs that are queried repeatedly
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE MATERIALIZED VIEW KAVYA_DB.KAVYA_ANALYTICS.MVW_DAILY_FRAUD_SUMMARY
COMMENT = 'Materialized view: pre-aggregates fraud metrics by date and country. Auto-refreshed by Snowflake. Best for dashboard KPI queries.'
AS
SELECT
    t.TRANSACTION_DATE,
    t.LOCATION_COUNTRY,
    COUNT(*)                                                        AS TOTAL_TRANSACTIONS,
    SUM(CASE WHEN t.IS_FRAUD_FLAGGED THEN 1    ELSE 0 END)         AS FRAUD_COUNT,
    SUM(CASE WHEN t.IS_FRAUD_FLAGGED THEN t.AMOUNT ELSE 0 END)     AS FRAUD_AMOUNT_GBP,
    ROUND(
        SUM(CASE WHEN t.IS_FRAUD_FLAGGED THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                               AS FRAUD_RATE_PCT,
    ROUND(AVG(t.FRAUD_SCORE), 2)                                   AS AVG_FRAUD_SCORE
FROM KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS t
GROUP BY t.TRANSACTION_DATE, t.LOCATION_COUNTRY;

-- Test materialized view — served from pre-computed store
SELECT * FROM KAVYA_DB.KAVYA_ANALYTICS.MVW_DAILY_FRAUD_SUMMARY
ORDER BY TRANSACTION_DATE DESC, FRAUD_COUNT DESC;


-- ── 5.4 NON-MATERIALIZED VIEW ────────────────────────────────
-- Purpose     : Fully enriched transaction view with business logic
-- Storage     : NONE — virtual, always reads live from base tables
-- Performance : Slower than materialized for aggregations
--               but always returns the most current data
-- Use case    : Detailed transaction queries where freshness matters
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW KAVYA_DB.KAVYA_ANALYTICS.VW_TRANSACTION_ENRICHED
COMMENT = 'Non-materialized view: enriched transactions with customer, account, FX context and risk classification. Always fresh, no storage cost.'
AS
SELECT
    t.TRANSACTION_ID,
    t.TRANSACTION_DATE,
    t.TRANSACTION_TIMESTAMP,
    t.TRANSACTION_TYPE,
    t.TRANSACTION_CATEGORY,
    t.MERCHANT_NAME,
    t.AMOUNT                                    AS AMOUNT_GBP,
    t.CURRENCY_CODE,
    t.CHANNEL,
    t.LOCATION_CITY,
    t.LOCATION_COUNTRY,
    t.IS_FRAUD_FLAGGED,
    t.FRAUD_SCORE,
    t.STATUS,
    -- Account context
    a.ACCOUNT_TYPE,
    a.CURRENT_BALANCE,
    -- Customer context
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME         AS CUSTOMER_NAME,
    c.CUSTOMER_SEGMENT,
    c.CREDIT_SCORE,
    -- FX enrichment: convert GBP to USD
    fx.EXCHANGE_RATE                            AS GBP_TO_USD_RATE,
    ROUND(t.AMOUNT * fx.EXCHANGE_RATE, 2)       AS AMOUNT_USD,
    -- Business logic: risk classification inline
    CASE
        WHEN t.FRAUD_SCORE >= 80 THEN 'CRITICAL'
        WHEN t.FRAUD_SCORE >= 60 THEN 'HIGH'
        WHEN t.FRAUD_SCORE >= 30 THEN 'MEDIUM'
        ELSE                          'LOW'
    END                                         AS RISK_LEVEL
FROM      KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS    t
JOIN      KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS        a  ON t.ACCOUNT_ID    = a.ACCOUNT_ID
JOIN      KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS       c  ON a.CUSTOMER_ID   = c.CUSTOMER_ID
LEFT JOIN KAVYA_DB.KAVYA_RAW.RAW_EXCHANGE_RATES  fx ON t.TRANSACTION_DATE = fx.RATE_DATE
                                                    AND t.CURRENCY_CODE   = fx.BASE_CURRENCY
                                                    AND fx.TARGET_CURRENCY = 'USD';

-- Test non-materialized view
SELECT * FROM KAVYA_DB.KAVYA_ANALYTICS.VW_TRANSACTION_ENRICHED
ORDER BY TRANSACTION_TIMESTAMP DESC;


-- ============================================================
-- VIEW COMPARISON SUMMARY
-- ============================================================

/*
┌──────────────────────────┬──────────────┬──────────────────┬─────────────────────────────────────┐
│ View Type                │ Storage Cost │ Performance      │ Best Use Case                       │
├──────────────────────────┼──────────────┼──────────────────┼─────────────────────────────────────┤
│ Standard View            │ None         │ Medium           │ Simple joins, infrequent ad-hoc use │
│ Secure View              │ None         │ Medium (slower)  │ PII masking, GDPR compliance        │
│ Materialized View        │ Yes          │ Fastest          │ Dashboard KPIs, repeated aggregates │
│ Non-Materialized View    │ None         │ Slower but fresh │ Complex logic, always-current data  │
└──────────────────────────┴──────────────┴──────────────────┴─────────────────────────────────────┘
*/


-- ============================================================
-- FINAL VERIFICATION
-- ============================================================

-- List all streams created
SHOW STREAMS IN SCHEMA KAVYA_DB.KAVYA_RAW;

-- List all tasks created
SHOW TASKS IN SCHEMA KAVYA_DB.KAVYA_RAW;

-- List all views in ANALYTICS schema
SHOW VIEWS IN SCHEMA KAVYA_DB.KAVYA_ANALYTICS;

-- List CDC stage tables
SHOW TABLES IN SCHEMA KAVYA_DB.KAVYA_STAGE;

-- Quick query to confirm all 4 views return data
SELECT 'VW_CUSTOMER_ACCOUNT_SUMMARY'  AS view_name, COUNT(*) AS rows FROM KAVYA_DB.KAVYA_ANALYTICS.VW_CUSTOMER_ACCOUNT_SUMMARY  UNION ALL
SELECT 'VW_SECURE_CUSTOMER_PII',       COUNT(*) FROM KAVYA_DB.KAVYA_ANALYTICS.VW_SECURE_CUSTOMER_PII                             UNION ALL
SELECT 'MVW_DAILY_FRAUD_SUMMARY',      COUNT(*) FROM KAVYA_DB.KAVYA_ANALYTICS.MVW_DAILY_FRAUD_SUMMARY                            UNION ALL
SELECT 'VW_TRANSACTION_ENRICHED',      COUNT(*) FROM KAVYA_DB.KAVYA_ANALYTICS.VW_TRANSACTION_ENRICHED;

-- ============================================================
-- PHASE 3B COMPLETE ✓
--
-- Time Travel       → Updated CUST005, deleted & restored ACC010
-- Zero Copy Clone   → Cloned KAVYA_RAW to KAVYA_RAW_CLONE
-- Streams & Tasks   → 3 streams + 3 tasks CDC pipeline running
-- Views             → Standard, Secure, Materialized, Non-Materialized
--
-- Next → PHASE 4 — dbt Project Setup
-- ============================================================
