-- ******************************************************************
-- DDL SQL Queries for the data lakehouse bronz layer
-- Author: ahmad Muhammad
-- Date: 2-8-2024
-- Version: 1.0
-- ******************************************************************
-- Make sure to refrence above each query with it's name

-- CREATE_TRANSACTIONS_TABLE
CREATE OR REPLACE VIEW nessie.bronz_transactions AS
SELECT
    transaction_id          STRING,
    sender_id               STRING,
    receiver_id             STRING,
    CAST(transaction_amount AS DOUBLE) AS transaction_amount,
    transaction_currency    STRING,
    transaction_date        TIMESTAMP,
    transaction_type        STRING,
    transaction_location    STRING,
    device_id               STRING
FROM nessie.bronze_raw_transactions;


-- CREATE_CLIENTS_TABLE
CREATE OR REPLACE VIEW nessie.bronze_clients AS
SELECT * FROM nessie.bronze_raw_clients;

-- CREATE_FRAUD_TRANSACTIONS_TABLE
CREATE OR REPLACE VIEW nessie.bronz_fraud_tranactions AS
SELECT * FROM nessie.bronz_raw_fraud_tranactions;