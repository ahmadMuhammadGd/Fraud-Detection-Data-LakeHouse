CREATE TABLE IF NOT EXISTS nessie.bronz_raw_fraud_tranactions (
    transaction_id          STRING,
    labeled_at              TIMESTAMP,
) USING iceberg
PARTITIONED BY (MONTH(labeled_at))