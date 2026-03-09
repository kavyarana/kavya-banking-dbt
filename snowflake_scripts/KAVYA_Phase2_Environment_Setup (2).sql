-- ============================================================
--  KAVYA RETAIL BANKING ANALYTICS PLATFORM
--  PHASE 2 — Snowflake Environment Setup
--  Account : mtiqioo-tq35930.snowflakecomputing.com
--  Author  : Kavya Rana
-- ============================================================
--
--  WHAT WE ARE CREATING:
--  ─────────────────────────────────────────────────────────
--  DATABASE    : KAVYA_DB
--  SCHEMA 1    : KAVYA_RAW        → Raw landing zone (never transformed)
--  SCHEMA 2    : KAVYA_STAGE      → Cleaned & standardised data
--  SCHEMA 3    : KAVYA_ANALYTICS  → Star schema marts for BI reporting
--  WAREHOUSE 1 : KAVYA_LOAD_WH    → Used for raw data ingestion
--  WAREHOUSE 2 : KAVYA_TRANSFORM_WH → Used by dbt transformations
--  WAREHOUSE 3 : KAVYA_ANALYTICS_WH → Used for BI / reporting queries
--
--  WHY 3 SCHEMAS?
--  ─────────────────────────────────────────────────────────
--  Separation of concerns:
--    RAW       → Immutable source data, append-only, never modified
--    STAGE     → Cleaned, typed, deduplicated by dbt staging models
--    ANALYTICS → Business-ready star schema exposed to BI tools
--  Each schema has a different lifecycle, owner and access pattern.
--
--  WHY 3 WAREHOUSES?
--  ─────────────────────────────────────────────────────────
--  KAVYA_LOAD_WH      SMALL   : Burst loading of CSV files, short-lived
--  KAVYA_TRANSFORM_WH SMALL   : dbt model runs, Streams & Tasks CDC
--  KAVYA_ANALYTICS_WH X-SMALL : Ad-hoc BI reads, low compute needed
--  Separate warehouses prevent ingestion from competing with BI queries.
-- ============================================================


-- ============================================================
-- STEP 1 — USE ACCOUNTADMIN FOR SETUP
-- ============================================================

USE ROLE ACCOUNTADMIN;


-- ============================================================
-- STEP 2 — CREATE WAREHOUSES
-- ============================================================

-- LOAD Warehouse: used for loading raw CSV data into KAVYA_RAW
CREATE WAREHOUSE IF NOT EXISTS KAVYA_LOAD_WH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 120
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Ingestion warehouse — loads raw CSV data into KAVYA_RAW schema';

-- TRANSFORM Warehouse: used by dbt and Snowflake Tasks for transformations
CREATE WAREHOUSE IF NOT EXISTS KAVYA_TRANSFORM_WH
    WAREHOUSE_SIZE      = 'SMALL'
    AUTO_SUSPEND        = 120
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'dbt transformation warehouse — runs staging, intermediate and mart models';

-- ANALYTICS Warehouse: used for reporting queries and BI dashboards
CREATE WAREHOUSE IF NOT EXISTS KAVYA_ANALYTICS_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics warehouse — read-only BI queries and dashboard access';


-- ============================================================
-- STEP 3 — CREATE DATABASE
-- ============================================================

CREATE DATABASE IF NOT EXISTS KAVYA_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Kavya Retail Banking Analytics Platform — main project database';


-- ============================================================
-- STEP 4 — CREATE SCHEMAS
-- ============================================================

-- RAW Schema: immutable landing zone — data loaded exactly as received from source
-- Tables here are permanent with Time Travel enabled
CREATE SCHEMA IF NOT EXISTS KAVYA_DB.KAVYA_RAW
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Raw landing zone — immutable source data, never modified after load';

-- STAGE Schema: cleaned, typed and deduplicated data
-- dbt staging models materialise here as views
CREATE SCHEMA IF NOT EXISTS KAVYA_DB.KAVYA_STAGE
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Staging layer — standardised, typed, deduplicated data ready for modelling';

-- ANALYTICS Schema: star schema mart — business-facing dimensional models
-- dbt mart models materialise here as tables and incremental models
CREATE SCHEMA IF NOT EXISTS KAVYA_DB.KAVYA_ANALYTICS
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Analytics mart — star schema dimensions and fact tables for BI reporting';


-- ============================================================
-- STEP 5 — SET WORKING CONTEXT
-- ============================================================

USE WAREHOUSE KAVYA_LOAD_WH;
USE DATABASE  KAVYA_DB;
USE SCHEMA    KAVYA_DB.KAVYA_RAW;


-- ============================================================
-- STEP 6 — VERIFY EVERYTHING WAS CREATED CORRECTLY
-- ============================================================

-- Check all warehouses
SHOW WAREHOUSES LIKE 'KAVYA%';

-- Check database
SHOW DATABASES LIKE 'KAVYA%';

-- Check all 3 schemas inside KAVYA_DB
SHOW SCHEMAS IN DATABASE KAVYA_DB;

-- ============================================================
-- PHASE 2 COMPLETE ✓
-- Database    : KAVYA_DB
-- Schemas     : KAVYA_RAW | KAVYA_STAGE | KAVYA_ANALYTICS
-- Warehouses  : KAVYA_LOAD_WH | KAVYA_TRANSFORM_WH | KAVYA_ANALYTICS_WH
--
-- Next → Run KAVYA_Phase3A_Raw_Tables.sql
-- ============================================================
