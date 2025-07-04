from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow.utils.task_group import TaskGroup

from common        import default_args
from tasks.scoring import (
    prepare_training_data, train_nova_model, evaluate_and_register
)

def decide_retrain(**context):
    """Skip si le gain attendu < 0 ,5 %."""
    improvement = evaluate_and_register(dry_run=True)
    return "train_group" if improvement >= 0.005 else "skip_retrain"

with DAG(
    dag_id="foodix_weekly_ml",
    start_date=datetime(2025, 1, 5),
    schedule="0 2 * * SUN",
    default_args=default_args,
    catchup=False,
    tags=["foodix", "ml"],
) as dag:

    decide = BranchPythonOperator(
        task_id="should_retrain",
        python_callable=decide_retrain,
    )

    skip = EmptyOperator(task_id="skip_retrain")

    with TaskGroup("train_group") as train_group:
        prep = PythonOperator(
            task_id="prepare_training_data",
            python_callable=prepare_training_data,
        )
        train = PythonOperator(
            task_id="train_model",
            python_callable=train_nova_model,
        )
        register = PythonOperator(
            task_id="evaluate_and_register",
            python_callable=evaluate_and_register,
        )
        prep >> train >> register

    decide >> train_group
    decide >> skip
