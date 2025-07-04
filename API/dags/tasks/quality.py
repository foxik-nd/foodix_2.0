from common import HDFS_ROOT
from pyspark.sql import SparkSession

def check_user_uniqueness(**ctx):
    spark = SparkSession.builder.appName("foodix_user_unique").getOrCreate()
    df = spark.read.format("parquet").load(f"{HDFS_ROOT}/users/{ctx['ds']}")
    dup = (df.groupBy("user_id").count().filter("count > 1").count())
    if dup > 0:
        raise ValueError(f"{dup} user_id en double dans la livraison {ctx['ds']}")

def detect_nutri_outliers(**ctx):
    spark = SparkSession.builder.appName("foodix_outliers").getOrCreate()
    df = spark.read.format("delta").load(f"{HDFS_ROOT}/products/delta")
    bad = df.filter("energy_100g > 6000 OR fat_100g > 100 OR carbs_100g > 100")
    if bad.count() > 0:
        sample = bad.select("ean", "energy_100g").limit(5).toJSON().collect()
        raise ValueError(f"Valeurs nutritionnelles aberrantes : {sample}")
