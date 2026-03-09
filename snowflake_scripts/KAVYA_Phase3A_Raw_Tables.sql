-- ============================================================
--  KAVYA RETAIL BANKING ANALYTICS PLATFORM
--  PHASE 3A — Raw Layer Design (Step 3)
--  8 Raw Tables with Sample Data
--  Author : Kavya Rana
-- ============================================================
--
--  TABLE SUMMARY:
--  ─────────────────────────────────────────────────────────
--  1. RAW_CUSTOMERS          → Customer master data
--  2. RAW_ACCOUNTS           → Bank accounts per customer
--  3. RAW_TRANSACTIONS       → All financial transactions
--  4. RAW_FRAUD_ALERTS       → Fraud investigation cases
--  5. RAW_PRODUCTS           → Banking product catalogue
--  6. RAW_BRANCHES           → Physical and digital branches
--  7. RAW_CUSTOMER_PRODUCTS  → Customer-product subscriptions
--  8. RAW_EXCHANGE_RATES     → Daily FX rates (GBP base)
--
--  DESIGN DECISIONS:
--  ─────────────────────────────────────────────────────────
--  PERMANENT tables   → Core entities needing Time Travel + Fail-safe
--  TRANSIENT table    → RAW_EXCHANGE_RATES (re-loadable, no fail-safe needed)
--  CLUSTERING         → RAW_TRANSACTIONS  (by date + account_id)
--                       RAW_ACCOUNTS      (by type + status)
--  DATA RETENTION     → 7 days on all permanent tables
--  _SOURCE_FILE       → Audit column: tracks which file loaded the row
--  _LOAD_TIMESTAMP    → Audit column: when the row was loaded
-- ============================================================

USE ROLE      ACCOUNTADMIN;
USE WAREHOUSE KAVYA_LOAD_WH;
USE DATABASE  KAVYA_DB;
USE SCHEMA    KAVYA_DB.KAVYA_RAW;


-- ============================================================
-- TABLE 1: RAW_CUSTOMERS
-- Grain   : One row per unique customer
-- Type    : Permanent (core master data — needs Time Travel)
-- PK      : CUSTOMER_ID
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(20)     NOT NULL,
    FIRST_NAME          VARCHAR(100)    NOT NULL,
    LAST_NAME           VARCHAR(100)    NOT NULL,
    DATE_OF_BIRTH       DATE,
    GENDER              VARCHAR(10),
    EMAIL               VARCHAR(255),
    PHONE_NUMBER        VARCHAR(30),
    ADDRESS_LINE1       VARCHAR(255),
    CITY                VARCHAR(100),
    COUNTRY             VARCHAR(100),
    POSTCODE            VARCHAR(20),
    ACCOUNT_OPEN_DATE   DATE,
    CUSTOMER_SEGMENT    VARCHAR(50),     -- RETAIL, PREMIUM, SME
    KYC_STATUS          VARCHAR(20),     -- VERIFIED, PENDING
    CREDIT_SCORE        NUMBER(5,0),
    IS_ACTIVE           BOOLEAN          DEFAULT TRUE,
    _SOURCE_FILE        VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw customer master data — grain: one row per customer. Permanent table with 7-day Time Travel.';


-- ============================================================
-- TABLE 2: RAW_ACCOUNTS
-- Grain     : One row per bank account (customer can have many)
-- Type      : Permanent
-- PK        : ACCOUNT_ID
-- Clustering: ACCOUNT_TYPE, ACCOUNT_STATUS (most common filters)
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS (
    ACCOUNT_ID          VARCHAR(20)     NOT NULL,
    CUSTOMER_ID         VARCHAR(20)     NOT NULL,
    ACCOUNT_TYPE        VARCHAR(50),     -- CURRENT, SAVINGS, ISA, LOAN
    ACCOUNT_STATUS      VARCHAR(20),     -- ACTIVE, CLOSED, FROZEN
    OPEN_DATE           DATE,
    CLOSE_DATE          DATE,
    CURRENCY_CODE       VARCHAR(3)       DEFAULT 'GBP',
    CURRENT_BALANCE     NUMBER(18,2),
    OVERDRAFT_LIMIT     NUMBER(18,2)     DEFAULT 0,
    INTEREST_RATE       NUMBER(6,4),
    BRANCH_ID           VARCHAR(20),
    SORT_CODE           VARCHAR(10),
    _SOURCE_FILE        VARCHAR(500),
    _LOAD_TIMESTAMP     TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_ACCOUNTS PRIMARY KEY (ACCOUNT_ID)
)
CLUSTER BY (ACCOUNT_TYPE, ACCOUNT_STATUS)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw bank accounts — grain: one row per account. Clustered by ACCOUNT_TYPE and ACCOUNT_STATUS for filter performance.';


-- ============================================================
-- TABLE 3: RAW_TRANSACTIONS
-- Grain     : One row per financial transaction
-- Type      : Permanent (largest table — must support Time Travel)
-- PK        : TRANSACTION_ID
-- Clustering: TRANSACTION_DATE, ACCOUNT_ID (date-range + account queries)
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS (
    TRANSACTION_ID          VARCHAR(36)     NOT NULL,
    ACCOUNT_ID              VARCHAR(20)     NOT NULL,
    TRANSACTION_DATE        DATE            NOT NULL,
    TRANSACTION_TIMESTAMP   TIMESTAMP_NTZ   NOT NULL,
    TRANSACTION_TYPE        VARCHAR(50),     -- DEBIT, CREDIT, TRANSFER, REFUND
    TRANSACTION_CATEGORY    VARCHAR(100),    -- GROCERIES, TRANSPORT, SALARY etc.
    MERCHANT_NAME           VARCHAR(255),
    MERCHANT_CATEGORY_CODE  VARCHAR(10),
    AMOUNT                  NUMBER(18,2)    NOT NULL,
    CURRENCY_CODE           VARCHAR(3)       DEFAULT 'GBP',
    CHANNEL                 VARCHAR(50),     -- ONLINE, ATM, BRANCH, CONTACTLESS
    LOCATION_CITY           VARCHAR(100),
    LOCATION_COUNTRY        VARCHAR(100),
    IS_FRAUD_FLAGGED        BOOLEAN          DEFAULT FALSE,
    FRAUD_SCORE             NUMBER(5,2),
    REFERENCE_NUMBER        VARCHAR(100),
    STATUS                  VARCHAR(20),     -- COMPLETED, PENDING, REVERSED
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_TRANSACTIONS PRIMARY KEY (TRANSACTION_ID)
)
CLUSTER BY (TRANSACTION_DATE, ACCOUNT_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw transactions — grain: one row per transaction. Clustered by TRANSACTION_DATE and ACCOUNT_ID for time-range query performance.';


-- ============================================================
-- TABLE 4: RAW_FRAUD_ALERTS
-- Grain : One row per fraud investigation case
-- Type  : Permanent
-- PK    : ALERT_ID
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_FRAUD_ALERTS (
    ALERT_ID                VARCHAR(36)     NOT NULL,
    TRANSACTION_ID          VARCHAR(36)     NOT NULL,
    ACCOUNT_ID              VARCHAR(20)     NOT NULL,
    ALERT_TIMESTAMP         TIMESTAMP_NTZ   NOT NULL,
    ALERT_TYPE              VARCHAR(100),    -- VELOCITY_CHECK, GEO_ANOMALY, AMOUNT_SPIKE
    ALERT_SEVERITY          VARCHAR(20),     -- LOW, MEDIUM, HIGH, CRITICAL
    FRAUD_SCORE             NUMBER(5,2),
    IS_CONFIRMED_FRAUD      BOOLEAN          DEFAULT FALSE,
    INVESTIGATION_STATUS    VARCHAR(50),     -- OPEN, CLOSED, ESCALATED
    RESOLVED_AT             TIMESTAMP_NTZ,
    RESOLVED_BY             VARCHAR(100),
    NOTES                   VARCHAR(2000),
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_FRAUD_ALERTS PRIMARY KEY (ALERT_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw fraud alerts — grain: one row per fraud investigation alert linked to a transaction.';


-- ============================================================
-- TABLE 5: RAW_PRODUCTS
-- Grain : One row per banking product
-- Type  : Permanent
-- PK    : PRODUCT_ID
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_PRODUCTS (
    PRODUCT_ID              VARCHAR(20)     NOT NULL,
    PRODUCT_NAME            VARCHAR(255)    NOT NULL,
    PRODUCT_TYPE            VARCHAR(50),     -- ACCOUNT, LOAN, CARD, INSURANCE
    PRODUCT_CATEGORY        VARCHAR(100),
    INTEREST_RATE           NUMBER(6,4),
    MONTHLY_FEE             NUMBER(10,2)     DEFAULT 0,
    MIN_BALANCE             NUMBER(18,2)     DEFAULT 0,
    MAX_CREDIT_LIMIT        NUMBER(18,2),
    ELIGIBILITY_CRITERIA    VARCHAR(1000),
    IS_ACTIVE               BOOLEAN          DEFAULT TRUE,
    LAUNCH_DATE             DATE,
    DISCONTINUE_DATE        DATE,
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_PRODUCTS PRIMARY KEY (PRODUCT_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw banking products catalogue — grain: one row per product.';


-- ============================================================
-- TABLE 6: RAW_BRANCHES
-- Grain : One row per physical or digital branch
-- Type  : Permanent
-- PK    : BRANCH_ID
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_BRANCHES (
    BRANCH_ID               VARCHAR(20)     NOT NULL,
    BRANCH_NAME             VARCHAR(255)    NOT NULL,
    BRANCH_TYPE             VARCHAR(50),     -- PHYSICAL, DIGITAL
    ADDRESS_LINE1           VARCHAR(255),
    CITY                    VARCHAR(100),
    REGION                  VARCHAR(100),
    COUNTRY                 VARCHAR(100)     DEFAULT 'United Kingdom',
    POSTCODE                VARCHAR(20),
    SORT_CODE               VARCHAR(10),
    OPEN_DATE               DATE,
    IS_ACTIVE               BOOLEAN          DEFAULT TRUE,
    MANAGER_NAME            VARCHAR(255),
    PHONE_NUMBER            VARCHAR(30),
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_BRANCHES PRIMARY KEY (BRANCH_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw branch master data — grain: one row per branch (physical or digital).';


-- ============================================================
-- TABLE 7: RAW_CUSTOMER_PRODUCTS
-- Grain : One row per customer-product subscription
-- Type  : Permanent
-- PK    : SUBSCRIPTION_ID
-- ============================================================

CREATE OR REPLACE TABLE KAVYA_DB.KAVYA_RAW.RAW_CUSTOMER_PRODUCTS (
    SUBSCRIPTION_ID         VARCHAR(36)     NOT NULL,
    CUSTOMER_ID             VARCHAR(20)     NOT NULL,
    PRODUCT_ID              VARCHAR(20)     NOT NULL,
    ACCOUNT_ID              VARCHAR(20),
    SUBSCRIPTION_DATE       DATE,
    EXPIRY_DATE             DATE,
    STATUS                  VARCHAR(20),     -- ACTIVE, EXPIRED, CANCELLED
    MONTHLY_CHARGE          NUMBER(10,2),
    DISCOUNT_APPLIED        NUMBER(5,2)      DEFAULT 0,
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_CUSTOMER_PRODUCTS PRIMARY KEY (SUBSCRIPTION_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Raw customer-product subscriptions — grain: one row per customer-product pairing.';


-- ============================================================
-- TABLE 8: RAW_EXCHANGE_RATES
-- Grain     : One row per currency pair per date
-- Type      : TRANSIENT (re-loadable reference data — no fail-safe needed)
-- PK        : RATE_DATE + BASE_CURRENCY + TARGET_CURRENCY
-- Why transient? FX rates are always reloadable from source.
--               Fail-safe storage cost is unnecessary for this data.
-- ============================================================

CREATE OR REPLACE TRANSIENT TABLE KAVYA_DB.KAVYA_RAW.RAW_EXCHANGE_RATES (
    RATE_DATE               DATE            NOT NULL,
    BASE_CURRENCY           VARCHAR(3)      NOT NULL,   -- Always GBP
    TARGET_CURRENCY         VARCHAR(3)      NOT NULL,
    EXCHANGE_RATE           NUMBER(18,6)    NOT NULL,
    RATE_SOURCE             VARCHAR(100),               -- ECB, OPEN_EXCHANGE_RATES
    _SOURCE_FILE            VARCHAR(500),
    _LOAD_TIMESTAMP         TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_RAW_EXCHANGE_RATES PRIMARY KEY (RATE_DATE, BASE_CURRENCY, TARGET_CURRENCY)
)
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'TRANSIENT: Daily FX rates — grain: one currency pair per day. Transient = no fail-safe cost as data is always re-loadable from source.';


-- ============================================================
-- SAMPLE DATA — Simulating Kaggle dataset load
-- Dataset: Bank Transaction Dataset for Fraud Detection
-- https://www.kaggle.com/datasets/valakhorasani/bank-transaction-dataset-for-fraud-detection
-- ============================================================


-- ── INSERT: RAW_CUSTOMERS (10 rows) ──────────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, GENDER,
     EMAIL, PHONE_NUMBER, ADDRESS_LINE1, CITY, COUNTRY, POSTCODE,
     ACCOUNT_OPEN_DATE, CUSTOMER_SEGMENT, KYC_STATUS, CREDIT_SCORE,
     IS_ACTIVE, _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('CUST001','Emma',    'Johnson','1988-03-15','F','emma.johnson@email.com',   '07700900001','12 Baker St',     'London',     'United Kingdom','NW1 6XE','2019-01-10','PREMIUM','VERIFIED',780,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST002','Liam',    'Smith',  '1995-07-22','M','liam.smith@email.com',     '07700900002','45 High St',      'Manchester', 'United Kingdom','M1 1AD', '2020-06-15','RETAIL', 'VERIFIED',650,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST003','Sophia',  'Patel',  '1990-11-03','F','sophia.patel@email.com',   '07700900003','8 Park Rd',       'Birmingham', 'United Kingdom','B1 1BB', '2018-09-20','RETAIL', 'VERIFIED',710,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST004','Oliver',  'Brown',  '1982-05-18','M','oliver.brown@email.com',   '07700900004','99 Victoria Rd',  'Leeds',      'United Kingdom','LS1 5BT','2017-03-01','SME',    'VERIFIED',820,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST005','Amelia',  'Wilson', '2000-12-30','F','amelia.wilson@email.com',  '07700900005','3 Queen St',      'Edinburgh',  'United Kingdom','EH1 1JQ','2022-08-14','RETAIL', 'PENDING', 590,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST006','Noah',    'Taylor', '1975-09-09','M','noah.taylor@email.com',    '07700900006','21 Church Lane',  'Bristol',    'United Kingdom','BS1 4AA','2015-05-05','PREMIUM','VERIFIED',760,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST007','Isabella','Davies', '1993-04-25','F','isabella.davies@email.com','07700900007','67 Mill Rd',      'Cambridge',  'United Kingdom','CB1 2AZ','2021-11-30','RETAIL', 'VERIFIED',680,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST008','George',  'Evans',  '1987-08-14','M','george.evans@email.com',   '07700900008','14 Elm Ave',      'Liverpool',  'United Kingdom','L1 8JQ', '2019-07-19','SME',    'VERIFIED',730,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST009','Mia',     'Thomas', '1998-02-07','F','mia.thomas@email.com',     '07700900009','5 Rose Gdns',     'Cardiff',    'United Kingdom','CF10 1EP','2023-01-20','RETAIL', 'PENDING', 610,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP()),
('CUST010','Harry',   'Roberts','1970-06-11','M','harry.roberts@email.com',  '07700900010','38 Oak St',       'Sheffield',  'United Kingdom','S1 2GG', '2012-02-28','PREMIUM','VERIFIED',800,TRUE,'2024-01_customers.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_ACCOUNTS (12 rows) ───────────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS
    (ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE, ACCOUNT_STATUS,
     OPEN_DATE, CLOSE_DATE, CURRENCY_CODE, CURRENT_BALANCE,
     OVERDRAFT_LIMIT, INTEREST_RATE, BRANCH_ID, SORT_CODE,
     _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('ACC001','CUST001','CURRENT','ACTIVE', '2019-01-10',NULL,   'GBP', 4250.00, 500.00,0.0000,'BR001','20-00-01','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC002','CUST001','SAVINGS','ACTIVE', '2019-01-10',NULL,   'GBP',15000.00,   0.00,0.0350,'BR001','20-00-01','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC003','CUST002','CURRENT','ACTIVE', '2020-06-15',NULL,   'GBP', 1100.50, 250.00,0.0000,'BR002','20-00-02','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC004','CUST003','CURRENT','ACTIVE', '2018-09-20',NULL,   'GBP', 3300.75, 500.00,0.0000,'BR003','20-00-03','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC005','CUST003','ISA',    'ACTIVE', '2020-04-06',NULL,   'GBP', 8500.00,   0.00,0.0320,'BR003','20-00-03','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC006','CUST004','CURRENT','ACTIVE', '2017-03-01',NULL,   'GBP',22000.00,5000.00,0.0000,'BR004','20-00-04','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC007','CUST005','CURRENT','ACTIVE', '2022-08-14',NULL,   'GBP',  450.00,   0.00,0.0000,'BR005','20-00-05','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC008','CUST006','SAVINGS','ACTIVE', '2015-05-05',NULL,   'GBP',45000.00,   0.00,0.0425,'BR006','20-00-06','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC009','CUST007','CURRENT','ACTIVE', '2021-11-30',NULL,   'GBP', 2100.00, 300.00,0.0000,'BR007','20-00-07','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC010','CUST008','CURRENT','FROZEN', '2019-07-19',NULL,   'GBP',  670.20,   0.00,0.0000,'BR008','20-00-08','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC011','CUST009','CURRENT','ACTIVE', '2023-01-20',NULL,   'GBP',  320.00,   0.00,0.0000,'BR009','20-00-09','2024-01_accounts.csv',CURRENT_TIMESTAMP()),
('ACC012','CUST010','CURRENT','ACTIVE', '2012-02-28',NULL,   'GBP', 9800.00,2000.00,0.0000,'BR010','20-00-10','2024-01_accounts.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_TRANSACTIONS (16 rows) ───────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS
    (TRANSACTION_ID, ACCOUNT_ID, TRANSACTION_DATE, TRANSACTION_TIMESTAMP,
     TRANSACTION_TYPE, TRANSACTION_CATEGORY, MERCHANT_NAME, MERCHANT_CATEGORY_CODE,
     AMOUNT, CURRENCY_CODE, CHANNEL, LOCATION_CITY, LOCATION_COUNTRY,
     IS_FRAUD_FLAGGED, FRAUD_SCORE, REFERENCE_NUMBER, STATUS,
     _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('TXN001','ACC001','2024-03-01','2024-03-01 08:15:00','DEBIT',   'GROCERIES',          'Tesco',             '5411',  52.30,'GBP','CONTACTLESS','London',   'United Kingdom',FALSE, 5.2,'REF001','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN002','ACC001','2024-03-02','2024-03-02 12:00:00','CREDIT',  'SALARY',             'Monzo Payroll',     '6012',4500.00,'GBP','ONLINE',     'London',   'United Kingdom',FALSE, 1.0,'REF002','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN003','ACC003','2024-03-02','2024-03-02 09:30:00','DEBIT',   'TRANSPORT',          'TfL',               '7011',   3.50,'GBP','CONTACTLESS','London',   'United Kingdom',FALSE, 2.1,'REF003','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN004','ACC006','2024-03-03','2024-03-03 14:00:00','DEBIT',   'BUSINESS',           'Amazon Business',   '5943',1250.00,'GBP','ONLINE',     'Leeds',    'United Kingdom',FALSE,10.5,'REF004','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN005','ACC007','2024-03-03','2024-03-03 23:45:00','DEBIT',   'GAMBLING',           'Unknown Merchant',  '7995', 800.00,'GBP','ONLINE',     'Unknown',  'Russia',        TRUE, 87.3,'REF005','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN006','ACC001','2024-03-04','2024-03-04 11:00:00','DEBIT',   'DINING',             'Dishoom',           '5812',  65.00,'GBP','CONTACTLESS','London',   'United Kingdom',FALSE, 3.4,'REF006','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN007','ACC004','2024-03-04','2024-03-04 16:30:00','DEBIT',   'UTILITIES',          'British Gas',       '4911', 120.00,'GBP','ONLINE',     'Birmingham','United Kingdom',FALSE, 1.5,'REF007','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN008','ACC008','2024-03-05','2024-03-05 09:00:00','CREDIT',  'INTEREST',           'Kavya Bank',        '6012', 159.38,'GBP','ONLINE',     'Bristol',  'United Kingdom',FALSE, 0.5,'REF008','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN009','ACC009','2024-03-05','2024-03-05 13:20:00','DEBIT',   'SHOPPING',           'ASOS',              '5691',  89.99,'GBP','ONLINE',     'Cambridge','United Kingdom',FALSE, 4.1,'REF009','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN010','ACC003','2024-03-06','2024-03-06 07:45:00','DEBIT',   'ATM_WITHDRAWAL',     'ATM NatWest',       '6011', 200.00,'GBP','ATM',        'Manchester','United Kingdom',FALSE, 3.0,'REF010','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN011','ACC001','2024-03-06','2024-03-06 17:10:00','DEBIT',   'GROCERIES',          'Sainsburys',        '5411',  34.60,'GBP','CONTACTLESS','London',   'United Kingdom',FALSE, 2.0,'REF011','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN012','ACC006','2024-03-07','2024-03-07 10:00:00','TRANSFER','INTERNAL_TRANSFER',  'Internal',          '9999',5000.00,'GBP','ONLINE',     'Leeds',    'United Kingdom',FALSE, 1.0,'REF012','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN013','ACC011','2024-03-07','2024-03-07 20:00:00','DEBIT',   'ENTERTAINMENT',      'Netflix',           '7922',  17.99,'GBP','ONLINE',     'Cardiff',  'United Kingdom',FALSE, 1.2,'REF013','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN014','ACC012','2024-03-08','2024-03-08 08:30:00','DEBIT',   'GROCERIES',          'Waitrose',          '5411',  78.45,'GBP','CONTACTLESS','Sheffield','United Kingdom',FALSE, 2.5,'REF014','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN015','ACC007','2024-03-08','2024-03-08 22:15:00','DEBIT',   'ONLINE_PURCHASE',    'Unknown Merchant',  '5999',1500.00,'GBP','ONLINE',     'Unknown',  'Nigeria',       TRUE, 92.7,'REF015','PENDING',  '2024-03_txn.csv',CURRENT_TIMESTAMP()),
('TXN016','ACC001','2024-03-09','2024-03-09 10:00:00','DEBIT',   'GROCERIES',          'M&S Food',          '5411',  42.10,'GBP','CONTACTLESS','London',   'United Kingdom',FALSE, 1.8,'REF016','COMPLETED','2024-03_txn.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_FRAUD_ALERTS (3 rows) ────────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_FRAUD_ALERTS
    (ALERT_ID, TRANSACTION_ID, ACCOUNT_ID, ALERT_TIMESTAMP,
     ALERT_TYPE, ALERT_SEVERITY, FRAUD_SCORE, IS_CONFIRMED_FRAUD,
     INVESTIGATION_STATUS, RESOLVED_AT, RESOLVED_BY, NOTES,
     _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('ALRT001','TXN005','ACC007','2024-03-03 23:46:00','GEO_ANOMALY',    'HIGH',    87.3,FALSE,'OPEN',      NULL,                    NULL,     'Transaction from unusual country: Russia',              '2024-03_fraud.csv',CURRENT_TIMESTAMP()),
('ALRT002','TXN015','ACC007','2024-03-08 22:16:00','AMOUNT_SPIKE',   'CRITICAL',92.7,TRUE, 'ESCALATED', NULL,                    NULL,     'Second high-value international transaction from Nigeria','2024-03_fraud.csv',CURRENT_TIMESTAMP()),
('ALRT003','TXN004','ACC006','2024-03-03 14:05:00','VELOCITY_CHECK', 'LOW',     10.5,FALSE,'CLOSED',    '2024-03-04 09:00:00',   'System', 'High value business spend — within customer profile',   '2024-03_fraud.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_PRODUCTS (6 rows) ────────────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_PRODUCTS
    (PRODUCT_ID, PRODUCT_NAME, PRODUCT_TYPE, PRODUCT_CATEGORY,
     INTEREST_RATE, MONTHLY_FEE, MIN_BALANCE, MAX_CREDIT_LIMIT,
     ELIGIBILITY_CRITERIA, IS_ACTIVE, LAUNCH_DATE, DISCONTINUE_DATE,
     _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('PROD001','Kavya Current Account', 'ACCOUNT','CURRENT_ACCOUNT',0.0000, 0.00,    0.00,  NULL,    'Age 18+, UK resident',          TRUE,'2010-01-01',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP()),
('PROD002','Kavya Premium Account', 'ACCOUNT','CURRENT_ACCOUNT',0.0000,15.00,    0.00,  NULL,    'Age 18+, income 35k+',          TRUE,'2012-06-01',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP()),
('PROD003','Kavya Savings Account', 'ACCOUNT','SAVINGS_ACCOUNT', 0.0350, 0.00,   1.00,  NULL,    'Must hold a current account',   TRUE,'2010-01-01',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP()),
('PROD004','Kavya ISA',             'ACCOUNT','ISA',             0.0320, 0.00,   1.00,20000.00,  'Age 18+, UK resident',          TRUE,'2011-04-06',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP()),
('PROD005','Kavya Personal Loan',   'LOAN',  'PERSONAL_LOAN',   0.0699, 0.00,   0.00,25000.00,  'Credit score 650+',             TRUE,'2013-01-01',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP()),
('PROD006','Kavya Business Account','ACCOUNT','BUSINESS_ACCOUNT',0.0000,10.00,   0.00,  NULL,    'Registered UK business',        TRUE,'2014-03-01',NULL,'2024-01_products.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_BRANCHES (10 rows) ───────────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_BRANCHES
    (BRANCH_ID, BRANCH_NAME, BRANCH_TYPE, ADDRESS_LINE1, CITY,
     REGION, COUNTRY, POSTCODE, SORT_CODE, OPEN_DATE,
     IS_ACTIVE, MANAGER_NAME, PHONE_NUMBER, _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('BR001','London Central',        'PHYSICAL','1 Finsbury Square', 'London',     'Greater London',   'United Kingdom','EC2A 1AE','20-00-01','2010-01-01',TRUE,'James Carter',     '020 7000 0001','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR002','Manchester Piccadilly', 'PHYSICAL','5 Piccadilly',      'Manchester', 'Greater Manchester','United Kingdom','M1 1AE', '20-00-02','2011-03-01',TRUE,'Sarah Hughes',     '0161 000 0002','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR003','Birmingham Central',    'PHYSICAL','10 New St',         'Birmingham', 'West Midlands',    'United Kingdom','B2 4EU', '20-00-03','2012-06-01',TRUE,'Raj Sharma',       '0121 000 0003','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR004','Leeds City',            'PHYSICAL','3 The Headrow',     'Leeds',      'West Yorkshire',   'United Kingdom','LS1 5RD','20-00-04','2013-09-01',TRUE,'Patricia Moon',    '0113 000 0004','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR005','Edinburgh Royal Mile',  'PHYSICAL','120 High St',       'Edinburgh',  'Scotland',         'United Kingdom','EH1 1SG','20-00-05','2014-04-01',TRUE,'Andrew MacLeod',   '0131 000 0005','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR006','Bristol Harbourside',   'PHYSICAL','2 Anchor Rd',       'Bristol',    'Avon',             'United Kingdom','BS1 5TT','20-00-06','2015-05-01',TRUE,'Claire Evans',     '0117 000 0006','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR007','Cambridge Digital',     'DIGITAL', NULL,                'Cambridge',  'Cambridgeshire',   'United Kingdom', NULL,    '20-00-07','2020-01-01',TRUE,'Digital Support',  'support@kavya.com','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR008','Liverpool Lime St',     'PHYSICAL','8 Lime St',         'Liverpool',  'Merseyside',       'United Kingdom','L1 1JQ', '20-00-08','2013-07-01',TRUE,'Tom Walsh',        '0151 000 0008','2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR009','Cardiff Bay',           'PHYSICAL','6 Bute Crescent',   'Cardiff',    'Wales',            'United Kingdom','CF10 5AL','20-00-09','2016-02-01',TRUE,'Sioned Griffiths', '029 000 0009', '2024-01_branches.csv',CURRENT_TIMESTAMP()),
('BR010','Sheffield Fargate',     'PHYSICAL','15 Fargate',        'Sheffield',  'South Yorkshire',  'United Kingdom','S1 2HE', '20-00-10','2014-11-01',TRUE,'Mark Booth',       '0114 000 0010','2024-01_branches.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_CUSTOMER_PRODUCTS (10 rows) ──────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_CUSTOMER_PRODUCTS
    (SUBSCRIPTION_ID, CUSTOMER_ID, PRODUCT_ID, ACCOUNT_ID,
     SUBSCRIPTION_DATE, EXPIRY_DATE, STATUS, MONTHLY_CHARGE,
     DISCOUNT_APPLIED, _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('SUB001','CUST001','PROD002','ACC001','2019-01-10',NULL,'ACTIVE',15.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB002','CUST001','PROD003','ACC002','2019-01-10',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB003','CUST002','PROD001','ACC003','2020-06-15',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB004','CUST003','PROD001','ACC004','2018-09-20',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB005','CUST003','PROD004','ACC005','2020-04-06',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB006','CUST004','PROD006','ACC006','2017-03-01',NULL,'ACTIVE',10.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB007','CUST005','PROD001','ACC007','2022-08-14',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB008','CUST006','PROD002','ACC008','2015-05-05',NULL,'ACTIVE',15.00,5.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB009','CUST007','PROD001','ACC009','2021-11-30',NULL,'ACTIVE', 0.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP()),
('SUB010','CUST010','PROD002','ACC012','2012-02-28',NULL,'ACTIVE',15.00,0.00,'2024-01_subs.csv',CURRENT_TIMESTAMP());


-- ── INSERT: RAW_EXCHANGE_RATES (9 rows) ──────────────────────

INSERT INTO KAVYA_DB.KAVYA_RAW.RAW_EXCHANGE_RATES
    (RATE_DATE, BASE_CURRENCY, TARGET_CURRENCY, EXCHANGE_RATE, RATE_SOURCE, _SOURCE_FILE, _LOAD_TIMESTAMP)
VALUES
('2024-03-01','GBP','USD',1.268500,'ECB','2024-03-01_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-01','GBP','EUR',1.171200,'ECB','2024-03-01_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-01','GBP','INR',105.4230,'ECB','2024-03-01_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-02','GBP','USD',1.270100,'ECB','2024-03-02_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-02','GBP','EUR',1.172500,'ECB','2024-03-02_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-02','GBP','INR',105.6110,'ECB','2024-03-02_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-03','GBP','USD',1.265800,'ECB','2024-03-03_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-03','GBP','EUR',1.169900,'ECB','2024-03-03_fx.csv',CURRENT_TIMESTAMP()),
('2024-03-03','GBP','INR',104.9800,'ECB','2024-03-03_fx.csv',CURRENT_TIMESTAMP());


-- ============================================================
-- VERIFICATION — Row counts across all 8 tables
-- ============================================================

SELECT 'RAW_CUSTOMERS'         AS table_name, COUNT(*) AS row_count FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMERS         UNION ALL
SELECT 'RAW_ACCOUNTS',                         COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_ACCOUNTS                      UNION ALL
SELECT 'RAW_TRANSACTIONS',                      COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_TRANSACTIONS                  UNION ALL
SELECT 'RAW_FRAUD_ALERTS',                      COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_FRAUD_ALERTS                  UNION ALL
SELECT 'RAW_PRODUCTS',                          COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_PRODUCTS                      UNION ALL
SELECT 'RAW_BRANCHES',                          COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_BRANCHES                      UNION ALL
SELECT 'RAW_CUSTOMER_PRODUCTS',                 COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_CUSTOMER_PRODUCTS             UNION ALL
SELECT 'RAW_EXCHANGE_RATES',                    COUNT(*) FROM KAVYA_DB.KAVYA_RAW.RAW_EXCHANGE_RATES
ORDER BY table_name;

-- ============================================================
-- PHASE 3A COMPLETE ✓
-- Expected row counts:
--   RAW_CUSTOMERS         → 10 rows
--   RAW_ACCOUNTS          → 12 rows
--   RAW_TRANSACTIONS      → 16 rows
--   RAW_FRAUD_ALERTS      →  3 rows
--   RAW_PRODUCTS          →  6 rows
--   RAW_BRANCHES          → 10 rows
--   RAW_CUSTOMER_PRODUCTS → 10 rows
--   RAW_EXCHANGE_RATES    →  9 rows
--
-- Next → Run KAVYA_Phase3B_Advanced_Features.sql
--        (Time Travel, Zero Copy Clone, Streams & Tasks, Views)
-- ============================================================
