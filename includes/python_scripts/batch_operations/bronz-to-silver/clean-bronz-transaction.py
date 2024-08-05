import sys
sys.path.insert(1, '/')

import logging
logger = logging.getLogger(__name__)

from includes.modules.SparkIcebergNessieMinIO.spark_setup import init_spark_session
from includes.modules.Data_cleaner.Strategies import (
    DropDuplicatesStrategy,
    DropMissingValuesStrategy,
    FilterNegativeValuesStrategy
)
from includes.modules.Data_cleaner.Interface import CleaningPipeline

spark = init_spark_session(app_name="clean bronz transactions")
sc = spark.sparkContext
sc.setLogLevel("WARN")
try:
    batch = spark.sql("""
        WITH max_silver_date AS (
            SELECT 
                COALESCE(MAX(transaction_date), DATE '1970-01-01') AS max_date 
            FROM 
                nessie.silver_transactions
        ),
        batch_raw_trans AS (
            SELECT 
                *
            FROM 
                nessie.bronz_raw_transactions AS b
            WHERE 
                CAST(b.transaction_datetime AS DATE) > (SELECT max_date FROM max_silver_date)
        )
        SELECT 
            b.*
        FROM 
            batch_raw_trans AS b
        LEFT JOIN 
            nessie.silver_transactions AS s
        ON 
            b.transaction_id = s.transaction_id
            AND s.transaction_date >= (SELECT max_date FROM max_silver_date)
        WHERE 
            s.transaction_id IS NULL;
    """)
    
    batch.show()

    if batch is None:
        raise ValueError("The DataFrame 'batch' is None. Check the data source and distination.")

    cleaner = CleaningPipeline()
    cleaner.set_dataframe(df=batch)

    cleaning_strategies = [
        DropDuplicatesStrategy(),
        DropMissingValuesStrategy(columns=[
            'transaction_id',
            'sender_id',
            'receiver_id',
            'transaction_amount',
            'transaction_currency',
            'transaction_type',
            'transaction_location',
            'device_id'
        ]),
        FilterNegativeValuesStrategy(columns=[
            'transaction_amount'
        ]),
        # Add more as needed, but that is enough as a starting point
    ]

    cleaner.add_strategy(cleaning_strategies)
    cleaned_batch = cleaner.run()
    cleaned_batch.createOrReplaceTempView('transactions_temp_view')

    spark.sql("""
    INSERT INTO nessie.silver_transactions
    SELECT 
        transaction_id,
        sender_id,
        receiver_id,
        transaction_amount,
        transaction_currency,
        transaction_datetime,
        CAST(transaction_datetime AS DATE) AS transaction_date,
        m.month_name AS transaction_month,
        DATE_FORMAT(transaction_datetime, 'HH:mm:ss') AS transaction_time,
        HOUR(transaction_datetime) AS transaction_hour,
        transaction_type,
        transaction_location,
        device_id
    FROM
        transactions_temp_view v
    LEFT JOIN
        nessie.months_lookup as m
    ON 
        m.id = MONTH(v.transaction_datetime)
    """)

    logger.info("Data cleaning and writing to Silver layer completed successfully.")
except Exception as e:
    logger.error(f"An error occurred: {e}")
finally:
    spark.stop()