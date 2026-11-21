
from pyspark.sql import SparkSession
from pathlib import Path
from uuid import uuid4

# Recuperer les identifiants MinIO depuis .env
credentials = {}

for line in Path("/workspace/.env").read_text(
    encoding="utf-8-sig"
).splitlines():
    if line.strip() and not line.strip().startswith("#"):
        key, sep, value = line.partition("=")
        if sep:
            credentials[key.strip()] = value.strip().strip('"').strip("'")

# Configuration Spark et MinIO
spark = (
    SparkSession.builder
    .appName("SahlPay_Test_MinIO")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "768m")
    .config("spark.cores.max", "2")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",
            credentials["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key",
            credentials["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

path = "s3a://lake/tests/s3a-" + uuid4().hex

try:
    print("\n===== TEST SPARK - MINIO =====")

    # Creer un petit RDD de test
    rdd = sc.parallelize(["SahlPay", "MinIO OK"], 2)

    # Ecriture distribuee dans MinIO
    rdd.saveAsTextFile(path)
    print("Ecriture MinIO : OK")

    # Lecture distribuee depuis MinIO
    result = sc.textFile(path).collect()
    print("Lecture MinIO :", result)

    assert sorted(result) == sorted(["SahlPay", "MinIO OK"])

    print("Chemin :", path)
    print("TEST REUSSI")

finally:
    spark.stop()
