-- ============================================================================
--  setup.sql   —   run ONCE in a Snowflake worksheet before starting.
--  Creates the stage, the raw landing table, a "new rows" bookmark (stream),
--  a batch table, and the 3 final summary tables.
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
CREATE DATABASE  IF NOT EXISTS AIRFLOW_DB;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE  AIRFLOW_DB;
USE SCHEMA    PUBLIC;

-- 1) STAGE = the drop-box where your daily files land.
CREATE STAGE IF NOT EXISTS sales_stage;

-- 2) FILE FORMAT = how to read the CSV files (comma-separated, header row).
CREATE FILE FORMAT IF NOT EXISTS sales_csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- 3) RAW landing table = data lands here exactly as it is in the files.
CREATE TABLE IF NOT EXISTS raw_sales (
    order_id    INTEGER,
    order_date  DATE,
    product     STRING,
    region      STRING,
    quantity    INTEGER,
    unit_price  NUMBER
);

-- 4) STREAM = a "bookmark" that remembers which rows are NEW since last time.
--    This powers the incremental behaviour. Create it BEFORE loading data.
CREATE STREAM IF NOT EXISTS new_sales_stream ON TABLE raw_sales;

-- 5) BATCH table = a small holding area for just this run's new rows.
CREATE TABLE IF NOT EXISTS new_sales_batch (
    order_id    INTEGER,
    order_date  DATE,
    product     STRING,
    region      STRING,
    quantity    INTEGER,
    unit_price  NUMBER
);

-- 6) The 3 FINAL report tables (your output)
CREATE TABLE IF NOT EXISTS summary_by_region (
    region STRING, total_orders INTEGER, total_revenue NUMBER
);
CREATE TABLE IF NOT EXISTS summary_by_product (
    product STRING, total_orders INTEGER, total_revenue NUMBER
);
CREATE TABLE IF NOT EXISTS summary_by_day_region (
    order_date DATE, region STRING, total_orders INTEGER, total_revenue NUMBER
);

SELECT 'Setup complete - you are ready' AS status;
