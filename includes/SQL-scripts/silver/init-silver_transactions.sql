CREATE TABLE IF NOT EXISTS nessie.silver_transactions (
    transaction_id          STRING,
    sender_id               STRING,
    receiver_id             STRING,
    transaction_amount      DOUBLE,
    transaction_currency    STRING,
    transaction_datetime    TIMESTAMP,
    transaction_date        DATE,
    transaction_month       STRING,
    transaction_time        STRING,
    transaction_hour        INT,
    transaction_type        STRING,
    transaction_location    STRING,
    device_id               STRING
) USING iceberg
PARTITIONED BY (transaction_date)