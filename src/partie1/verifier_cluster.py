"""Partie 1 (PDF §3.1) : vérifier l'application connectée au cluster Spark."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SahlPay_Jalon1_Partie1")
    .master("spark://spark-master:7077")
    # Le Driver tourne dans le conteneur Jupyter : les Workers doivent le joindre.
    .config("spark.driver.host", "jupyter")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.port", "37001")
    .config("spark.blockManager.port", "37002")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "768m")
    .config("spark.cores.max", "2")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("\n===== SAHLPAY - PARTIE 1 =====")
print("Spark version     :", spark.version)
print("Spark master      :", sc.master)
print("Application name  :", sc.appName)
print("Default parallelism:", sc.defaultParallelism)
print("Application ID    :", sc.applicationId)
print("Test calcul distribue, somme de 0 a 99 :", sc.parallelize(range(100), 2).sum())
print("===== FIN DU TEST =====\n")

spark.stop()
