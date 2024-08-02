'''
---------------------------------------------------------------------
---------------------------------------------------------------------
|                                                                   |
|   THIS SCRIPT CONSUMES KAFKA DATA AT TOPIC 'bank-transactions'    |
|   THEN LOADS THE OUTPUT TO AN ICEBERG TABLE 'nessie.transactions' |
|   IN MINIO S3 BUCKET                                              |
|                                                                   |
---------------------------------------------------------------------
---------------------------------------------------------------------
'''

import sys, os
sys.path.insert(1, '/includes')

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from modules.SparkIcebergNessieMinIO import CustomSparkConfig

import dotenv
dotenv.load('/scripts-variables.env')
NESSIE_URI          = os.getenv('NESSIE_URI')
MINIO_ACCESS_KEY    = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY    = os.getenv('MINIO_SECRET_KEY')
MINIO_S3_BUCKET     = os.getenv('MINIO_S3_BUCKET')
MINIO_END_POINT     = os.getenv('MINIO_END_POINT')

with open('tansactions-stream-sample.json', 'r') as f:
    sample_json = f.read()

conf = CustomSparkConfig.IceBergNessieMinio(
            nessie_url              = NESSIE_URI,
            minio_access_key        = MINIO_ACCESS_KEY,
            minio_secret_key        = MINIO_SECRET_KEY,
            minio_s3_bucket_path    = MINIO_S3_BUCKET,
            minio_endpoint          = MINIO_END_POINT
        )\
        .init()


spark = SparkSession.builder\
    .appName("KafkaTransactionConsumer")\
    .config(conf=conf)\
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank-transactions") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")


json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema
df = df.select(from_json(col("json_value"), json_schema).alias("data")).select("data.*")
df = df.withColumn("transaction_date", to_timestamp(col("transaction_date")))


def foreach_batch_function(df, epoch_id):
    df.createOrReplaceTempView("transactions_temp")
    df.writeTo("nessie.transactions").append()

query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function) \
    .start()
    
query.awaitTermination()
