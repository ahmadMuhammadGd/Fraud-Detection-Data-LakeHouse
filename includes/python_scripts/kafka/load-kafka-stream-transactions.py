'''
---------------------------------------------------------------------
---------------------------------------------------------------------
|                                                                   |
|   THIS SCRIPT CONSUMES KAFKA DATA AT TOPIC 'bank-transactions'    |
|   THEN LOADS THE OUTPUT TO AN ICEBERG TABLE                       |
|   'nessie.bronz_raw_transactions' IN MINIO S3 BUCKET              |
|                                                                   |
---------------------------------------------------------------------
---------------------------------------------------------------------
'''

import sys
sys.path.insert(1, '/')

from pyspark.sql.functions import from_json, col, to_timestamp
from includes.modules.SparkIcebergNessieMinIO import spark_setup

with open('/includes/python_scripts/kafka/tansactions-stream-sample.json', 'r') as f:
    sample_json = f.read()

spark = spark_setup.init_spark_session(app_name="KafkaTransactionConsumer")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank-transactions") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")


json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema
df = df.select(from_json(col("json_value"), json_schema)
                .alias("data"))\
                .select("data.*")
df = df.withColumn("transaction_datetime", 
                   to_timestamp(col("transaction_datetime")))

def foreach_batch_function(df, epoch_id):
    df.writeTo("nessie.bronz_raw_transactions").append()


query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function) \
    .start()
    
query.awaitTermination()