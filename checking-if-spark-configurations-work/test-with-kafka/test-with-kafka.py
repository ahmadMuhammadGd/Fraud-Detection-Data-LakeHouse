from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark import SparkConf

NESSIE_URI = "http://nessie:19120/api/v1"
MINIO_ACCESS_KEY = "2UclcjKKJWDEveS04KtE"
MINIO_SECRET_KEY = "hwGSbiyR3ZKw7GKFr7cgjHgPKmYpTHCC8xI7AWpS"


conf = (
    SparkConf()
        .setAppName('minio-nessie-test-without-kafka')
        .set('spark.jars.packages', '''
            org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
            org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.92.1,\
            ''')
        
        .set('spark.sql.extensions', '''
            org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
            org.projectnessie.spark.extensions.NessieSparkSessionExtensions\
            ''')
        
        .set('spark.sql.catalog.nessie',                            'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri',                        NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref',                        'main')
        .set('spark.sql.catalog.nessie.authentication.type',        'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl',               'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse',                  's3://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint',                'http://172.18.0.3:9000')
        .set('spark.sql.catalog.nessie.io-impl',                    'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key',                      MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key',                      MINIO_SECRET_KEY)
)

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

# Convert the transaction_date from STRING to TIMESTAMP
df = df.withColumn("transaction_date", to_timestamp(col("transaction_date")))

spark.sql("DROP TABLE IF EXISTS nessie.transactions;")
def foreach_batch_function(df, epoch_id):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.transactions (
            transaction_id STRING,
            sender_id STRING,
            receiver_id STRING,
            transaction_amount DOUBLE,
            transaction_currency STRING,
            transaction_date TIMESTAMP,
            transaction_type STRING,
            transaction_location STRING,
            device_id STRING
        ) USING iceberg
    """)
    df.writeTo("nessie.transactions").append()

# Write stream to Iceberg
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function) \
    .start()
    
query.awaitTermination()
