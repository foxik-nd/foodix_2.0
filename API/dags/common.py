from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

HDFS_ROOT      = "hdfs://namenode:9870"
RAW_OFF_DIR    = "/data/raw/off"
REPORT_EMAIL   = "n.yazi@ecole-ipssi.net"

