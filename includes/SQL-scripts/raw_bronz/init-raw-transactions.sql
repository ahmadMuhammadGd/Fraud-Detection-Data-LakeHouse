CREATE TABLE IF NOT EXISTS nessie.bronz_raw_transactions (
    transaction_id          STRING,
    sender_id               STRING,
    receiver_id             STRING,
    transaction_amount      DOUBLE,
    transaction_currency    STRING,
    transaction_datetime    TIMESTAMP,
    transaction_type        STRING,
    transaction_location    STRING,
    device_id               STRING
) USING iceberg
PARTITIONED BY (DATE(transaction_datetime))