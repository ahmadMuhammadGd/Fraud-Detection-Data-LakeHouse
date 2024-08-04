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

try:
    batch = spark.sql("""
    WITH add_date_col AS (
        SELECT
            *,
            CAST (transaction_datetime as DATE) AS transaction_date
        FROM
            nessie.bronz_raw_transactions
        WHERE 
            transaction_datetime IS NOT NULL
    )
    SELECT *
    FROM 
        (
            SELECT *,
                RANK() OVER (ORDER BY transaction_date DESC) as rnk
            FROM add_date_col
        ) rankedDates
    WHERE 
        rankedDates.rnk = 1;
    """)

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
        transactions_temp_view AS t
    LEFT JOIN
        nessie.months_lookup AS m
    ON
        MONTH(t.transaction_datetime) = m.id
    """)

    logger.info("Data cleaning and writing to Silver layer completed successfully.")
except Exception as e:
    logger.error(f"An error occurred: {e}")
finally:
    spark.stop()