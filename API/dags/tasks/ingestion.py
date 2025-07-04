import json, pathlib, requests
from common import HDFS_ROOT, RAW_OFF_DIR

def extract_off(**ctx):
    """Télécharge la liste des produits modifiés depuis la veille."""
    ts  = ctx["ds"]
    url = f"https://world.openfoodfacts.org/api/v2/search?page_size=1000&updated_t={ts}"
    data = requests.get(url, timeout=30).json()
    pathlib.Path(RAW_OFF_DIR).mkdir(parents=True, exist_ok=True)
    with open(f"{RAW_OFF_DIR}/off_{ts}.json", "w") as f:
        json.dump(data, f)

def transform_products(**ctx):
    """Nettoie la donnée et pousse du Delta Parquet."""
    from pyspark.sql import SparkSession

    ts = ctx["ds"]
    spark = SparkSession.builder.appName("foodix_off_transform").getOrCreate()
    raw  = spark.read.json(f"{RAW_OFF_DIR}/off_{ts}.json")
    df   = (raw.selectExpr("code as ean", "*")
                 .dropDuplicates(["ean"])
                 .drop("code"))

    df.write.format("delta").mode("append") \
        .save(f"{HDFS_ROOT}/products/delta")
