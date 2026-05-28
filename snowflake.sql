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