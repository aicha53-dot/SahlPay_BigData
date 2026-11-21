# SahlPay.ma — Écosystème Big Data 2 | Jalon 1

Aicha EL BEKALI · Malak ACHARI — EMSI, 5e année, 2026–2027. Échéance : 22/11/2026.

## Périmètre et état
Les parties 1 (infrastructure), 2 (RDD) et 3 (DataFrames/SQL) ont été exécutées et validées sur le cluster local le 21/11/2026. Ce dossier consolide les scripts et corrections de configuration utilisés. Les preuves et mesures sont dans `captures/`, `results/` et `rapport/`. Le dossier consolidé n'a pas lui-même été redéployé dans un environnement vierge. Les parties 4–6 sont hors périmètre (20/12/2026).

Le générateur officiel et son référentiel n'ont pas été retrouvés au moment des essais : `producer.py` est notre substitut synthétique déterministe (seed 42), mode fichier uniquement. Il produit 50 000 événements dont 1 000 copies, 2 500 événements originaux retardés (ordre de livraison simulé), 300 commerçants et une vérité terrain pour les scénarios injectés. Si les fichiers officiels sont récupérés, les remplacer et recalculer les résultats. Le mode Kafka du générateur et l'évaluation des anomalies restent à réaliser au jalon 2.

## Architecture et versions
`producer.py -> JSONL -> MinIO Bronze -> Spark RDD / DataFrames / SQL -> MinIO Silver Parquet`
Kafka KRaft est prêt pour le deuxième jalon. Spark Standalone 3.5.7, Scala 2.12, Java 17 (17.0.16 observé), Python 3.10 dans Jupyter, PySpark 3.5.7, JupyterLab 4.4.9, Kafka 3.9.1, MinIO RELEASE.2025-09-07T16-13-09Z (binaire vérifié SHA-256), Hadoop client 3.3.4, hadoop-aws 3.3.4 et aws-java-sdk-bundle 1.12.262. Deux Workers, chacun 1 cœur / 1 Gio, Executors 768 Mio / 1 cœur. Déploiement local pédagogique (pas de haute disponibilité matérielle, broker unique, services HTTP en boucle locale ; Jupyter sans token, à ne pas exposer).

## Reproduction sous Windows PowerShell
Depuis le dossier du projet. Docker Desktop doit utiliser les conteneurs Linux. Ne pas écraser un `.env` existant.

```powershell
if (!(Test-Path .env)) { Copy-Item .env.example .env }
# Modifier les identifiants de démonstration dans .env ; ne jamais publier .env.
docker compose config --quiet
docker pull apache/spark:3.5.7-scala2.12-java17-python3-ubuntu
docker pull apache/kafka:3.9.1
docker compose build minio jupyter
# Initialiser les permissions du volume KRaft AVANT le 1er démarrage.
# Exécuter Kafka arrêté ; ne touche pas aux autres volumes.
docker compose run --rm --no-deps --user 0 --entrypoint /bin/sh kafka -c "chown -R 1000:1000 /tmp/kraft-combined-logs && chmod u+rwx /tmp/kraft-combined-logs"
docker compose up -d
docker compose ps
```

Attendre le démarrage du broker ; vérifier `docker compose logs --tail=30 kafka`. L'image MinIO est construite localement, car le pull Quay a échoué. Le Dockerfile Jupyter corrige le HOME `/nonexistent` de l'utilisateur spark. La commande d'initialisation Kafka corrige son accès refusé au répertoire KRaft.

- Spark Master : http://localhost:8080 (2 Workers ALIVE).
- MinIO : http://localhost:9001 (identifiants `.env`, créer le bucket privé `lake` s'il manque).
- JupyterLab : http://localhost:8888.
- Spark UI d'application : http://localhost:4040 pendant l'exécution.

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic transactions
docker compose exec jupyter python3 /workspace/src/partie1/verifier_cluster.py
```
Attendu : 3 partitions, Spark 3.5.7, master `spark://spark-master:7077`, parallélisme 2, somme 4950.

## Connecteur MinIO S3A
```powershell
New-Item -ItemType Directory -Force jars
curl.exe -fL "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" -o "jars/hadoop-aws-3.3.4.jar"
curl.exe -fL "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o "jars/aws-java-sdk-bundle-1.12.262.jar"
```
Les JAR ne sont pas versionnés. Ils sont transmis aux Executors avec `spark-submit --jars`.

Test facultatif de connectivité (script consolidé équivalent à celui exécuté sur le poste) :

```powershell
docker compose exec jupyter /opt/spark/bin/spark-submit --jars /workspace/jars/hadoop-aws-3.3.4.jar,/workspace/jars/aws-java-sdk-bundle-1.12.262.jar /workspace/src/partie2/test_minio.py
```

## Données et traitements
```powershell
python producer.py --mode file --n 50000 --output-dir data
(Get-Content data/transactions_history.jsonl | Measure-Object -Line).Lines
(Get-Content data/merchants.json | Measure-Object -Line).Lines
```
Dans la console MinIO, créer les préfixes et téléverser les fichiers :
- `lake/bronze/history/transactions_history.jsonl`
- `lake/bronze/reference/merchants.json`

```powershell
docker compose exec jupyter /opt/spark/bin/spark-submit --jars /workspace/jars/hadoop-aws-3.3.4.jar,/workspace/jars/aws-java-sdk-bundle-1.12.262.jar /workspace/src/partie2/analyse_rdd.py
# Attendre la fin de l'application (pause de 4 min pour les captures).
docker compose exec jupyter /opt/spark/bin/spark-submit --jars /workspace/jars/hadoop-aws-3.3.4.jar,/workspace/jars/aws-java-sdk-bundle-1.12.262.jar /workspace/src/partie3/analyse_dataframe_sql.py
```
Les jobs utilisent les deux cœurs ; lancer les applications l'une après l'autre. La Partie 3 attend 8 minutes pour les captures. Pour supprimer uniquement la pause, ajouter `-e SAHLPAY_NO_PAUSE=1` à `docker compose exec` (avant `jupyter`).

## Résultats et critères d'acceptation
- P1 : 2 Workers, topic 3 partitions, bucket `lake`, test Spark 4950.
- P2 : 50 000 événements, 14 697 COMPLETED, 10 villes, top 5 M-029 (51), M-066 (50), M-079 (50), M-035 (49), M-300 (49). 5 jobs / 8 stages / 16 tasks lors de l'essai 17:56 ; shuffle du total : 580 octets.
- P3 : égalité exacte des totaux RDD/DataFrame/SQL, 10 247 P2M joints sur 10 247, 60 agrégats catégorie-ville, BroadcastHashJoin, tests de limites UDF réussis. 49 000 lignes Silver relues après suppression de 1 000 doublons, 10 préfixes `city=`. `results/partie3_results.json` est généré à chaque exécution de la Partie 3 (conserver le fichier réel de votre poste).
- Benchmarks (essai 18:05, 3 répétitions sur données en cache) : médianes RDD 0,6202 s ; DataFrame 0,3680 s ; SQL 0,4452 s ; UDF 0,6091 s ; expression native 0,3883 s. Les captures détaillées SQL ont été prises lors d'un autre essai à 18:30. Ne pas mélanger les durées des deux exécutions.

Les agrégats batch de référence conservent les doublons et incluent P2M/P2P COMPLETED ; ce ne sont pas des recettes nettes après annulation. Silver conserve tous les statuts, valide les champs, enrichit et dédoublonne sur `(tx_id,status)`. La réécriture en mode overwrite concerne uniquement le chemin Silver historique. Dataset typé comparé théoriquement, pas benchmarké.

## Arrêt et reprise
```powershell
docker compose stop
# Reprise ultérieure
docker compose start
```
Ne pas utiliser `docker compose down -v` : perte des volumes. Ne pas publier `.env`, les JAR, ou les fichiers synthétiques volumineux. L'accès HTTP local et les credentials de démonstration ne conviennent pas à la production.

## Rendu
Voir `rapport/SahlPay_Jalon1.pdf` et les captures. Avant dépôt, vérifier les modalités du professeur et ajouter les preuves locales éventuellement manquantes (capture Kafka describe, SparkSession, interface Jupyter, fichier Parquet dans city=Rabat). Les logs textuels disponibles attestent déjà des tests correspondants, sauf l'ouverture navigateur Jupyter non documentée. Le rapport final des six parties reste limité à 10 pages ; condenser le rapport intermédiaire au besoin.
