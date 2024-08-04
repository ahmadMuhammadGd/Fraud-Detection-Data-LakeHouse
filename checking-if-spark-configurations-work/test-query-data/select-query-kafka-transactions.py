import sys
sys.path.insert(1, '/includes')

from pyspark.sql import SparkSession
from modules.SparkIcebergNessieMinIO import CustomSparkConfig

NESSIE_URI = "http://nessie:19120/api/v1"
MINIO_ACCESS_KEY = "2UclcjKKJWDEveS04KtE"
MINIO_SECRET_KEY = "hwGSbiyR3ZKw7GKFr7cgjHgPKmYpTHCC8xI7AWpS"


conf = CustomSparkConfig.IceBergNessieMinio(
            nessie_url              = NESSIE_URI,
            minio_access_key        = MINIO_ACCESS_KEY,
            minio_secret_key        = MINIO_SECRET_KEY,
            minio_s3_bucket_path    = 's3://warehouse',
            minio_endpoint          = 'http://172.18.0.2:9000'
        ).init()

spark = SparkSession.builder\
    .appName("KafkaTransactionConsumer")\
    .config(conf=conf)\
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

spark.sql(
    """
    SELECT * 
    FROM nessie.transactions
    WHERE transaction_amount > 90;
    """
).show()