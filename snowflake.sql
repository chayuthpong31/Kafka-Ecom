-- Create Database & Schema
CREATE DATABASE KAFKA_DB;
CREATE SCHEMA KAFKA_DB.STREAMING;

-- Create Table
CREATE TABLE kafka_events_silver(
    event_id STRING,
    customer_id STRING,
    event_type STRING,
    amount NUMBER(10, 2),
    currency STRING,
    event_timestamp TIMESTAMP_NTZ,
    kafka_ingest_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE VIEW daily_customer_revenue AS
SELECT
    customer_id,
    DATE(event_timestamp) AS event_date,
    SUM(amount) AS total_revenue,
    COUNT_IF(event_type = 'PURCHASE') AS purchase_count
FROM kafka_events_silver
WHERE event_type = 'PURCHASE'
GROUP BY customer_id, DATE(event_timestamp);

CREATE VIEW event_funnel AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT_IF(event_type = 'PAGE_VIEW') AS page_views,
    COUNT_IF(event_type = 'ADD_TO_CART') AS add_to_cart,
    COUNT_IF(event_type = 'PURCHASE') AS purchases
FROM kafka_events_silver
GROUP BY DATE(event_timestamp);