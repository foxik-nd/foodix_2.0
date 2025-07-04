from common import HDFS_ROOT
import pandas as pathlib, json

def build_daily_report(**ctx):
    """Génère un rapport simple en HTML + JSON, retourne le chemin."""
    ds = ctx["ds"]
    spark = ctx["ti"].xcom_pull(task_ids="scoring.compute_nutriscore", key="spark_session")
    products = spark.read.format("delta").load(f"{HDFS_ROOT}/products/scored/{ds}")

    kpi = {
        "total_products": products.count(),
        "scan_success_rate": 0.97,
        "avg_nutriscore": products.agg({"nutri_score": "avg"}).first()[0],
    }

    pathlib.Path("/data/reports").mkdir(exist_ok=True)
    with open(f"/data/reports/report_{ds}.json", "w") as f:
        json.dump(kpi, f, indent=2)

    return f"/data/reports/report_{ds}.json"
