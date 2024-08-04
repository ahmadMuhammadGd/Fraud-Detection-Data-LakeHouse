from pyspark import SparkConf


class IceBergNessieMinio:
    def __init__(self, nessie_url: str, minio_s3_bucket_path: str, minio_endpoint: str, minio_access_key: str, minio_secret_key: str) -> None:
        self.nessie_url = nessie_url
        self.minio_s3_bucket_path = minio_s3_bucket_path
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def init(self) -> SparkConf:
        spark_jars_packages = [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.92.1"
        ]

        spark_sql_extensions = [
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
        ]

        conf = SparkConf() \
            .set("spark.jars.packages"                                  , ",".join(spark_jars_packages)) \
            .set("spark.sql.extensions"                                 , ",".join(spark_sql_extensions)) \
            .set("spark.sql.catalog.nessie"                             , "org.apache.iceberg.spark.SparkCatalog") \
            .set("spark.sql.catalog.nessie.uri"                         , self.nessie_url) \
            .set("spark.sql.catalog.nessie.ref"                         , "main") \
            .set("spark.sql.catalog.nessie.authentication.type"         , "NONE") \
            .set("spark.sql.catalog.nessie.catalog-impl"                , "org.apache.iceberg.nessie.NessieCatalog") \
            .set("spark.sql.catalog.nessie.warehouse"                   , self.minio_s3_bucket_path) \
            .set("spark.sql.catalog.nessie.s3.endpoint"                 , self.minio_endpoint) \
            .set("spark.sql.catalog.nessie.io-impl"                     , "org.apache.iceberg.aws.s3.S3FileIO") \
            .set("spark.hadoop.fs.s3a.access.key"                       , self.minio_access_key) \
            .set("spark.hadoop.fs.s3a.secret.key"                       , self.minio_secret_key)\

        return conf
