import pyspark
from pyspark.sql import SparkSession
import os

# os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.94.4 pyspark-shell'


NESSIE_URI = "http://nessie:19120/api/v1"
MINIO_ACCESS_KEY = "2UclcjKKJWDEveS04KtE"
MINIO_SECRET_KEY = "hwGSbiyR3ZKw7GKFr7cgjHgPKmYpTHCC8xI7AWpS"


conf = (
    pyspark.SparkConf()
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
        .set('spark.sql.catalog.nessie.s3.endpoint',                'http://172.18.0.2:9000')
        .set('spark.sql.catalog.nessie.io-impl',                    'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key',                      MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key',                      MINIO_SECRET_KEY)
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark Running")

csv_df = spark.read.format("csv").option("header", "true").load("/includes/test-without-kafka/test.csv")
csv_df.createOrReplaceTempView("csv_open_2023")
spark.sql("CREATE TABLE IF NOT EXISTS nessie.df_open_2023_lesson2 USING iceberg AS SELECT * FROM csv_open_2023").show()