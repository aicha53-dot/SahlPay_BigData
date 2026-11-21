# Index des preuves du Jalon 1

Captures réelles transmises le 21 novembre 2026, sans reconstruction.

- 01–02 : Master et bucket MinIO.
- 06, 08, 18 : objets de test S3A, historique et référentiel Bronze.
- 12–16 : Jobs, DAG, stages, Executors et Master lors du traitement RDD.
- 22–28 : Jobs SQL (18:05), stages et requêtes (18:30), détails de la jointure et de l'agrégation (18:30).
- 29 : partitionnement Silver Parquet.

Les sorties de Kafka et de SparkSession sont résumées dans `../results/partie1_mesures.txt`. La sortie EXPLAIN et les benchmarks sont dans `../results/partie3_execution_1805.txt`.

À ajouter si disponibles sur le poste : capture Kafka describe, capture SparkSession, interface JupyterLab, fichiers Parquet dans city=Rabat, Executors SQL. Ne pas inventer de captures si l'application est arrêtée ; les preuves déjà fournies permettent de documenter les opérations.
