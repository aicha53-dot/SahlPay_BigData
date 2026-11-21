"""SahlPay - Partie 3 : DataFrames, SQL, Catalyst, benchmark et Silver."""
import json
import os
from decimal import Decimal
from pathlib import Path
from statistics import median
from time import perf_counter, sleep

from pyspark.sql import SparkSession, functions as F, types as T

credentials = {}
for line in Path('/workspace/.env').read_text(encoding='utf-8-sig').splitlines():
    if line.strip() and not line.strip().startswith('#'):
        key, sep, value = line.partition('=')
        if sep:
            credentials[key.strip()] = value.strip().strip('"').strip("'")

spark = (SparkSession.builder
    .appName('SahlPay_Jalon1_Partie3_SQL')
    .master('spark://spark-master:7077')
    .config('spark.driver.host', 'jupyter')
    .config('spark.driver.bindAddress', '0.0.0.0')
    .config('spark.executor.cores', '1')
    .config('spark.executor.memory', '768m')
    .config('spark.cores.max', '2')
    .config('spark.sql.shuffle.partitions', '2')
    .config('spark.sql.session.timeZone', 'UTC')
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
HISTORY = 's3a://lake/bronze/history/transactions_history.jsonl'
MERCHANTS = 's3a://lake/bronze/reference/merchants.json'
SILVER = 's3a://lake/silver/transactions_history/'

# Schemas explicites : le fichier merchants.json est lui aussi en JSON Lines.
tx_schema = T.StructType([
    T.StructField('tx_id', T.StringType()),
    T.StructField('type', T.StringType()),
    T.StructField('payer_wallet', T.StringType()),
    T.StructField('payee_id', T.StringType()),
    T.StructField('device_id', T.StringType()),
    T.StructField('city', T.StringType()),
    T.StructField('channel', T.StringType()),
    T.StructField('status', T.StringType()),
    T.StructField('amount_mad', T.DecimalType(12, 2)),
    T.StructField('event_time', T.TimestampType()),
])
merchant_schema = T.StructType([
    T.StructField('merchant_id', T.StringType()),
    T.StructField('name', T.StringType()),
    T.StructField('city', T.StringType()),
    T.StructField('category', T.StringType()),
    T.StructField('latitude', T.DoubleType()),
    T.StructField('longitude', T.DoubleType()),
])

def amount_band(amount):
    if amount is None:
        return None
    if amount < 50:
        return 'micro'
    if amount <= 2000:
        return 'standard'
    return 'eleve'

band_udf = F.udf(amount_band, T.StringType())

def band_native(col):
    return (F.when(col.isNull(), F.lit(None).cast('string'))
             .when(col < 50, 'micro')
             .when(col <= 2000, 'standard')
             .otherwise('eleve'))

def to_cents(e):
    return int(Decimal(str(e['amount_mad'])) * 100)

def df_totals(rows):
    return {r['city']: r['total_mad'] for r in rows}

def rdd_totals(rows):
    return {city: Decimal(cents)/100 for city, cents in rows}

def measure(label, func):
    start = perf_counter()
    result = func()
    elapsed = perf_counter() - start
    print(f'{label}: {elapsed:.4f} s', flush=True)
    return result, elapsed

try:
    print('\n===== PARTIE 3 : SCHEMAS EXPLICITES =====')
    tx = spark.read.schema(tx_schema).option('mode', 'FAILFAST').json(HISTORY).cache()
    merchants = spark.read.schema(merchant_schema).option('mode', 'FAILFAST').json(MERCHANTS).cache()
    tx.printSchema()
    merchants.printSchema()
    sc.setJobGroup('P3-01', 'Lecture et validation des schemas')
    n_raw, n_merchants = tx.count(), merchants.count()
    print('Evenements bruts :', n_raw, '| Commercants :', n_merchants)
    assert n_raw == 50000 and n_merchants == 300
    assert merchants.select('merchant_id').distinct().count() == n_merchants

    # Meme perimetre que Partie 2 : COMPLETED, doublons conserves.
    completed = tx.filter(F.col('status') == 'COMPLETED').cache()
    n_completed = completed.count()
    print('Evenements COMPLETED :', n_completed)
    completed.createOrReplaceTempView('completed_transactions')

    sc.setJobGroup('P3-02', 'Montant par ville - DataFrame')
    df_query = completed.groupBy('city').agg(F.sum('amount_mad').alias('total_mad'))
    df_result, df_first = measure('Temps action DataFrame (cache)', df_query.collect)
    print('\n===== TOTAUX DATAFRAME =====')
    for r in sorted(df_result, key=lambda r: r['city']):
        print(f"{r['city']:15} {r['total_mad']:15.2f} MAD")
    sc.setJobGroup('P3-03', 'Montant par ville - SQL')
    sql_query = spark.sql('''SELECT city, SUM(amount_mad) AS total_mad
                             FROM completed_transactions GROUP BY city''')
    sql_result, sql_first = measure('Temps action SQL (cache)', sql_query.collect)
    print('\n===== TOTAUX SQL =====')
    for r in sorted(sql_result, key=lambda r: r['city']):
        print(f"{r['city']:15} {r['total_mad']:15.2f} MAD")
    assert df_totals(df_result) == df_totals(sql_result)

    # Rejouer le meme calcul RDD pour un benchmark dans la meme application.
    sc.setJobGroup('P3-04', 'Benchmark RDD, DataFrame, SQL')
    completed_rdd = (sc.textFile(HISTORY, minPartitions=2).map(json.loads)
                     .filter(lambda e: e['status'] == 'COMPLETED').cache())
    assert completed_rdd.count() == n_completed
    def run_rdd():
        return (completed_rdd.map(lambda e: (e['city'], to_cents(e)))
                .reduceByKey(lambda a, b: a+b, numPartitions=2).collect())
    def run_df():
        return completed.groupBy('city').agg(F.sum('amount_mad').alias('total_mad')).collect()
    def run_sql():
        return spark.sql('SELECT city, SUM(amount_mad) AS total_mad FROM completed_transactions GROUP BY city').collect()
    expected = df_totals(df_result)
    assert rdd_totals(run_rdd()) == expected
    assert df_totals(run_df()) == expected
    assert df_totals(run_sql()) == expected
    bench = {k: [] for k in ('RDD','DataFrame','SQL')}
    funcs = {'RDD':run_rdd, 'DataFrame':run_df, 'SQL':run_sql}
    print('\n===== BENCHMARK : 3 ESSAIS SUR DONNEES EN CACHE =====')
    for order in [('RDD','DataFrame','SQL'), ('DataFrame','SQL','RDD'), ('SQL','RDD','DataFrame')]:
        for name in order:
            result, dt = measure(name, funcs[name])
            assert (rdd_totals(result) if name == 'RDD' else df_totals(result)) == expected
            bench[name].append(dt)
    for name, times in bench.items():
        print(f'{name:10} mediane={median(times):.4f} s')
    print('VALIDATION : RDD == DataFrame == SQL pour les 10 villes')
    completed_rdd.unpersist()

    # Jointure des P2M valides avec le referentiel (petite dimension broadcast).
    sc.setJobGroup('P3-05', 'Jointure P2M commerçants et Catalyst')
    dim = merchants.select('merchant_id', F.col('name').alias('merchant_name'),
                           F.col('city').alias('merchant_city'), 'category', 'latitude', 'longitude')
    p2m = completed.filter(F.col('type') == 'P2M')
    joined = p2m.alias('t').join(F.broadcast(dim).alias('m'), F.col('t.payee_id') == F.col('m.merchant_id'), 'inner')
    print('\n===== EXPLAIN TRUE - JOINTURE =====', flush=True)
    joined.explain(True)
    n_p2m = p2m.count()
    n_join = joined.count()
    assert n_join == n_p2m, 'Paiements P2M sans correspondant ou doublons du referentiel'
    breakdown = (joined.groupBy('category', 'merchant_city')
                 .agg(F.count('*').alias('nb_paiements'), F.sum('amount_mad').alias('total_mad'))
                 .orderBy('merchant_city', 'category'))
    print('\n===== MONTANT P2M PAR CATEGORIE ET VILLE COMMERCANT =====')
    breakdown.show(100, truncate=False)
    print('P2M COMPLETED :', n_p2m, '| P2M joints :', n_join)

    # UDF vs expression native : collect sur groupBy force le calcul de la bande.
    sc.setJobGroup('P3-06', 'UDF contre when otherwise')
    udf_df = tx.withColumn('amount_band', band_udf(F.col('amount_mad')))
    native_df = tx.withColumn('amount_band', band_native(F.col('amount_mad')))
    both = (tx.withColumn('udf_band', band_udf(F.col('amount_mad')))
            .withColumn('native_band', band_native(F.col('amount_mad'))))
    assert both.filter(~F.col('udf_band').eqNullSafe(F.col('native_band'))).count() == 0
    test_amounts = spark.createDataFrame([(Decimal('49.99'),),(Decimal('50.00'),),
        (Decimal('2000.00'),),(Decimal('2000.01'),),(None,)], 'amount_mad decimal(12,2)')
    test_rows = (test_amounts.withColumn('udf_band',band_udf(F.col('amount_mad')))
        .withColumn('native_band',band_native(F.col('amount_mad'))).collect())
    assert {r.amount_mad: r.udf_band for r in test_rows} == {
        Decimal('49.99'):'micro', Decimal('50.00'):'standard',
        Decimal('2000.00'):'standard', Decimal('2000.01'):'eleve', None:None}
    assert all(r.udf_band == r.native_band for r in test_rows)
    print('\n===== TESTS LIMITES AMOUNT_BAND : OK =====')
    for r in test_rows: print(r)
    def bands(df):
        return df.groupBy('amount_band').count().collect()
    bands(udf_df); bands(native_df)  # echauffement
    band_times = {'UDF': [], 'Native': []}
    print('\n===== BENCHMARK UDF VS WHEN/OTHERWISE =====')
    for order in [('UDF','Native'), ('Native','UDF'), ('UDF','Native')]:
        for name in order:
            result, dt = measure(name, lambda: bands(udf_df if name == 'UDF' else native_df))
            band_times[name].append(dt)
    for name, times in band_times.items():
        print(f'{name:10} mediane={median(times):.4f} s')
    native_df.groupBy('amount_band').count().orderBy('amount_band').show()

    # Silver batch: qualite + suppression des doublons en dehors du benchmark.
    # Tous les statuts sont conserves, pas seulement COMPLETED.
    sc.setJobGroup('P3-07', 'Nettoyage et ecriture Silver Parquet')
    valid = tx.dropna(subset=tx.columns).filter(
        F.col('type').isin('P2M','P2P') &
        F.col('status').isin('INITIATED','AUTHORIZED','COMPLETED','REJECTED','REVERSED') &
        F.col('channel').isin('APP','QR','USSD') & (F.col('amount_mad') > 0) &
        ((F.col('type') == 'P2M') & F.col('payee_id').startswith('M-') |
         (F.col('type') == 'P2P') & F.col('payee_id').startswith('W-')))
    n_valid = valid.count()
    # Les doublons du generateur sont des copies exactes. Refuser les conflits.
    unique_payloads = valid.dropDuplicates().count()
    unique_keys = valid.select('tx_id','status').distinct().count()
    assert unique_payloads == unique_keys, 'Conflits de contenu pour un meme tx_id/status'
    clean = valid.dropDuplicates(['tx_id','status']).withColumn('amount_band',band_native(F.col('amount_mad')))
    silver = clean.alias('t').join(F.broadcast(dim).alias('m'),
        (F.col('t.type') == 'P2M') & (F.col('t.payee_id') == F.col('m.merchant_id')), 'left')
    assert silver.filter((F.col('type')=='P2M') & F.col('merchant_id').isNull()).count() == 0
    print('\n===== SILVER HISTORIQUE =====')
    print('Bruts:', n_raw, '| Valides:', n_valid, '| Doublons retires:', n_valid-unique_keys)
    print('Ecriture Parquet partitionne par city:', SILVER)
    # Overwrite limite au chemin Silver batch, pour permettre de relancer le script.
    silver.write.mode('overwrite').partitionBy('city').parquet(SILVER)
    restored = spark.read.parquet(SILVER)
    n_silver = restored.count()
    assert n_silver == unique_keys
    assert restored.select('tx_id','status').distinct().count() == n_silver
    restored.groupBy('city').count().orderBy('city').show(20, truncate=False)
    print('Nombre de lignes Silver relues :', n_silver)
    print('VALIDATIONS PARTIE 3 REUSSIES', flush=True)

    output = Path('/workspace/results')
    output.mkdir(exist_ok=True)
    report = {'raw_events':n_raw, 'completed_events':n_completed, 'merchants':n_merchants,
              'valid_events':n_valid, 'silver_events':n_silver, 'duplicates_removed':n_valid-unique_keys,
              'p2m_completed':n_p2m, 'totals_mad':{k:str(v) for k,v in sorted(expected.items())},
              'benchmark_seconds':bench, 'benchmark_medians_seconds':{k:median(v) for k,v in bench.items()},
              'band_benchmark_seconds':band_times, 'band_medians_seconds':{k:median(v) for k,v in band_times.items()}}
    (output/'partie3_results.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('Resultats sauvegardes : results/partie3_results.json')
    if os.environ.get('SAHLPAY_NO_PAUSE') != '1':
        print('Spark UI : http://localhost:4040 - disponible pendant 8 minutes.', flush=True)
        sleep(480)
finally:
    spark.stop()
