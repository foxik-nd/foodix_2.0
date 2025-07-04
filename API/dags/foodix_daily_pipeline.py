from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

from common import default_args, RAW_OFF_DIR, REPORT_EMAIL
from tasks.ingestion import extract_off, transform_products
from tasks.quality import check_user_uniqueness, detect_nutri_outliers
from tasks.reporting import build_daily_report

with DAG(
    dag_id="foodix_daily_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["foodix", "daily"],
) as dag:
    with TaskGroup("ingestion", tooltip="Extraction + transform") as ingestion:
        extract = PythonOperator(
            task_id="extract_openfoodfacts",
            python_callable=extract_off,
        )

        transform = PythonOperator(
            task_id="transform_products",
            python_callable=transform_products,
        )

        archive = BashOperator(
            task_id="archive_raw",
            bash_command=f"gzip -f {RAW_OFF_DIR}/off_{{{{ ds }}}}.json",
        )

        extract >> transform >> archive

    with TaskGroup("quality_checks", tooltip="Règles qualité") as quality:
        uniq = PythonOperator(
            task_id="check_user_uniqueness",
            python_callable=check_user_uniqueness,
        )

        outliers = PythonOperator(
            task_id="detect_nutri_outliers",
            python_callable=detect_nutri_outliers,
        )

        uniq >> outliers

    with TaskGroup("reporting") as reporting:
        build = PythonOperator(
            task_id="build_report",
            python_callable=build_daily_report,
        )

        email = EmailOperator(
            task_id="send_report",
            to=REPORT_EMAIL,
            subject="[Foodix] Rapport quotidien {{ ds }}",
            html_content="""Fichier de reporting dispo sous {{ ti.xcom_pull(task_ids='reporting.build_report') }}""",
        )

        build >> email

    ingestion  >> quality >> reporting
