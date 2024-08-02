-- ******************************************************************
-- DDL SQL Queries for the data lakehouse raw bronz layer
-- Author: ahmad Muhammad
-- Date: 2-8-2024
-- Version: 1.0
-- ******************************************************************
-- Make sure to refrence above each query with it's name

-- CREATE_TRANSACTIONS_TABLE
CREATE TABLE IF NOT EXISTS nessie.bronz_raw_transactions (
    transaction_id          STRING,
    sender_id               STRING,
    receiver_id             STRING,
    transaction_amount      STRING,
    transaction_currency    STRING,
    transaction_date        TIMESTAMP,
    transaction_type        STRING,
    transaction_location    STRING,
    device_id               STRING
) USING iceberg
PARTITIONED BY (hours(transaction_date))

-- CREATE_CLIENTS_TABLE
CREATE TABLE IF NOT EXISTS nessie.bronz_raw_clients (
    first_name              STRING,
    last_name               STRING,
    phone_number            STRING,
    email                   STRING,
    c_address               STRING,
    birth_date              DATE,
    client_id               STRING,
    registration_date       TIMESTAMP,
) USING iceberg
PARTITIONED BY (MONTH(registration_date))

-- CREATE_FRAUD_TRANSACTIONS_TABLE
CREATE TABLE IF NOT EXISTS nessie.bronz_raw_fraud_tranactions (
    transaction_id          STRING,
    is_fraud                BOOLEAN,
    labeled_at              TIMESTAMP,
) USING iceberg
PARTITIONED BY (MONTH(labeled_at))