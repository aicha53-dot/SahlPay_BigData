"""SahlPay - Partie 2: historique Bronze, calculs SparkCore/RDD."""
import json
import os
from decimal import Decimal
from pathlib import Path
from time import perf_counter, sleep
from pyspark.sql import SparkSession

# Configuration locale: ne jamais afficher les identifiants.
credentials = {}
for line in Path('/workspace/.env').read_text(encoding='utf-8-sig').splitlines():
    if line.strip() and not line.strip().startswith('#'):
        key, sep, value = line.partition('=')
        if sep:
            credentials[key.strip()] = value.strip().strip('"').strip("'")

spark = (SparkSession.builder
    .appName('SahlPay_Jalon1_Partie2_RDD')
    .master('spark://spark-master:7077')
    .config('spark.driver.host', 'jupyter')
    .config('spark.driver.bindAddress', '0.0.0.0')
    .config('spark.executor.cores', '1')
    .config('spark.executor.memory', '768m')
    .config('spark.cores.max', '2')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', credentials['MINIO_ROOT_USER'])
    .config('spark.hadoop.fs.s3a.secret.key', credentials['MINIO_ROOT_PASSWORD'])
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel('WARN')

def cents(event):
    return int(Decimal(str(event['amount_mad'])) * 100)

try:
    path = 's3a://lake/bronze/history/transactions_history.jsonl'
    lines = sc.textFile(path, minPartitions=2)
    events = lines.map(json.loads)
    completed = events.filter(lambda e: e['status'] == 'COMPLETED').cache()
    sc.setJobGroup('P2-01', 'Comptage et preparation')
    raw_count = lines.count()
    completed_count = completed.count()
    print('\n===== SAHLPAY - PARTIE 2 RDD =====')
    print('Evenements bruts :', raw_count)
    print('Partitions en entree :', lines.getNumPartitions())
    print('Evenements COMPLETED :', completed_count)
    assert raw_count == 50000, f'Historique inattendu: {raw_count}'

    # Somme des montants COMPLETED par ville (P2M et P2P).
    sc.setJobGroup('P2-02', 'Montant total par ville - reduceByKey')
    totals = completed.map(lambda e: (e['city'], cents(e))).reduceByKey(lambda a, b: a+b, numPartitions=2)
    start = perf_counter()
    total_rows = totals.collect()  # Action; le tri suivant est local au Driver.
    elapsed = perf_counter() - start
    print('\n===== MONTANT TOTAL PAR VILLE =====')
    for city, total in sorted(total_rows):
        print(f'{city:15} {Decimal(total)/100:15.2f} MAD')
    print(f'Temps action RDD (COMPLETED deja en cache) : {elapsed:.4f} s')

    # Somme et nombre en une seule aggregation, puis moyenne.
    sc.setJobGroup('P2-03', 'Moyenne par ville - somme et nombre')
    stats = (completed.map(lambda e: (e['city'], (cents(e), 1)))
             .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]), numPartitions=2))
    stat_rows = stats.collect()
    print('\n===== MONTANT MOYEN PAR VILLE =====')
    print(f'{"Ville":15} {"Nombre":>8} {"Total MAD":>15} {"Moyenne MAD":>15}')
    for city, (total, n) in sorted(stat_rows):
        print(f'{city:15} {n:8d} {Decimal(total)/100:15.2f} {Decimal(total)/(100*n):15.2f}')
    assert sum(n for _,(_,n) in stat_rows) == completed_count
    assert dict(total_rows) == {city: total for city,(total,_) in stat_rows}

    # Top commerçants: uniquement paiements P2M validés.
    sc.setJobGroup('P2-04', 'Top 5 commerçants - takeOrdered')
    merchant_counts = (completed.filter(lambda e: e['type'] == 'P2M')
                       .map(lambda e: (e['payee_id'], 1))
                       .reduceByKey(lambda a,b: a+b, numPartitions=2))
    top5 = merchant_counts.takeOrdered(5, key=lambda pair: (-pair[1], pair[0]))
    print('\n===== TOP 5 COMMERCANTS =====')
    for rank, (mid, n) in enumerate(top5, 1):
        print(f'{rank}. {mid} : {n} paiements valides')

    # Lineage: description des dependances, sans nouveau calcul distribue.
    print('\n===== LINEAGE DU TOTAL PAR VILLE =====')
    print(totals.toDebugString().decode('utf-8'))
    print('\n===== VALIDATIONS INTERNES REUSSIES =====')
    print('Spark UI : http://localhost:4040')
    print('Les doublons sont conserves dans ce calcul batch de reference;')
    print('le dedoublonnage sera applique separement et avec les memes regles en Partie 3 si necessaire.')
    if os.environ.get('SAHLPAY_NO_PAUSE') != '1':
        print('\nSpark UI reste accessible pendant 4 minutes pour les captures.', flush=True)
        sleep(240)
finally:
    spark.stop()
