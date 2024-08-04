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
PARTITIONED BY (DAY(registration_date))