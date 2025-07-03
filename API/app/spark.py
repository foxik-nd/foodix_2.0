from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# 1. Créer la session Spark
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définir le schéma attendu du message JSON
schema = StructType() \
    .add("event", StringType()) \
    .add("user_id", StringType()) \
    .add("barcode", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("location", StringType()) \
    .add("device", StringType())

# 3. Lire depuis Kafka en stream
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "produits_scannes") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convertir le champ "value" (bytes) en string JSON
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

# 5. Parser le JSON en colonnes structurées
df_structured = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 6. (Optionnel) Appliquer un filtre, transformation, etc.
# Ex : ne garder que les produits Coca-Cola
# df_structured = df_structured.filter(col("barcode").startswith("5449"))

# 7. Écrire dans HDFS en format Parquet
query = (
    df_structured.writeStream
    .format("parquet")  # ou "json"
    .option("path", "hdfs://namenode:9000/data/produits_scannes/")
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/produits_scannes/")
    .outputMode("append")
    .start()
)

query.awaitTermination()
