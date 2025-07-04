from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

HDFS_LOGS_PATH = "hdfs://namenode:9000/logs/user_events.json"

spark = SparkSession.builder.appName("ScanSummary").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lire les logs
df_logs = spark.read.json(HDFS_LOGS_PATH)

# Ajouter la colonne "moment"
from pyspark.sql.functions import when, hour
df_logs = df_logs.withColumn(
    "moment",
    when(hour("timestamp") < 10, "petit_dejeuner")
    .when((hour("timestamp") >= 10) & (hour("timestamp") < 14), "dejeuner")
    .when((hour("timestamp") >= 14) & (hour("timestamp") < 21), "diner")
    .otherwise("autre")
)

# 1. Nombre de scans par utilisateur
print("Nombre de scans par utilisateur :")
df_logs.groupBy("user_id").count().show()

# Exemple pour le nombre de scans par utilisateur
df_logs.groupBy("user_id").count().write.mode("overwrite").json("hdfs://namenode:9000/summary/scans_by_user")

# 2. Nombre de scans par moment de la journée
print("Nombre de scans par moment de la journée :")
df_logs.groupBy("moment").count().show()

# 3. Top produits scannés (tous utilisateurs)
print("Top produits scannés :")
df_logs.groupBy("barcode").count().orderBy(desc("count")).show(10)

# 4. Top produits scannés par utilisateur
print("Top produits scannés par utilisateur :")
df_logs.groupBy("user_id", "barcode").count().orderBy("user_id", desc("count")).show(20)

df_logs.filter(col("user_id") == "yanis").groupBy("barcode").count().write.mode("overwrite").json("hdfs://namenode:9000/summary/yanis_scans")

spark.stop()