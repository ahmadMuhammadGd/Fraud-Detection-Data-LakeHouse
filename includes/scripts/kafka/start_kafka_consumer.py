from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("KafkaTransactionConsumer") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bank-transactions") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json_value")

sample_json = """
    {
        "transaction_id": "5eb18ca0-2690-4d5c-a8ca-a5b493f38655",
        "sender_id": "b4633b89-1490-4024-83e8-0e7fe2b6963d",
        "receiver_id": "47067c71-7f31-447f-aab1-139e2d511ad4",
        "transaction_amount": 48,
        "transaction_currency": "USD",
        "transaction_date": "2024-06-23T04:04:04.636397",
        "transaction_type": "purchase",
        "transaction_location": "Port Derekland",
        "device_id": "5f13f67e-c0f3-4677-9c45-6f10d65fb069"
    }
"""

json_schema = spark.read.json(spark.sparkContext.parallelize([sample_json])).schema

df = df.select(from_json(col("json_value"), json_schema).alias("data")).select("data.*")

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
