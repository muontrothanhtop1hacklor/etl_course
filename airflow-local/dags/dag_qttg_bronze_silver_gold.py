"""Airflow orchestration for the local QTTG Spark pipeline."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_CONTAINER = os.getenv("QTTG_SPARK_CONTAINER", "tst-spark-master")
SPARK_MASTER = os.getenv("QTTG_SPARK_MASTER", "spark://tst-spark-master:7077")
APP_DIR = os.getenv("QTTG_SPARK_APP_DIR", "/opt/spark/apps/qttg")
LAKE_DIR = os.getenv("QTTG_SPARK_LAKE_DIR", "file:///opt/spark/data/lake")
RAW_DIR = os.getenv("QTTG_SPARK_RAW_DIR", "file:///opt/spark/data/raw_qttg_1m")


def spark_submit(app_name: str, arguments: str = "") -> str:
    command = (
        f"docker exec {SPARK_CONTAINER} "
        "/opt/spark/bin/spark-submit "
        f"--master {SPARK_MASTER} "
        f"--deploy-mode client "
        f"--driver-memory 1g "
        f"--executor-memory 2g "
        f"--executor-cores 2 "
        f"{APP_DIR}/{app_name} {arguments}"
    )
    return f"set -e\n{command}"


def task_timeout(minutes: int = 60) -> timedelta:
    return timedelta(minutes=minutes)


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="qttg_bronze_silver_gold",
    description="QTTG ingest -> ETL -> monthly result -> validation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "qttg", "local"],
) as dag:
    bronze = BashOperator(
        task_id="bronze_qttg",
        bash_command=spark_submit(
            "bronze_qttg.py",
            f"--input-dir {RAW_DIR} --output-dir {LAKE_DIR}/bronze",
        ),
        execution_timeout=task_timeout(),
    )

    silver = BashOperator(
        task_id="silver_qttg",
        bash_command=spark_submit(
            "silver_qttg.py",
            f"--input-dir {LAKE_DIR}/bronze --output-dir {LAKE_DIR}/silver",
        ),
        execution_timeout=task_timeout(),
    )

    gold = BashOperator(
        task_id="gold_qttg",
        bash_command=spark_submit(
            "gold_qttg.py",
            f"--input-dir {LAKE_DIR}/silver --output-dir {LAKE_DIR}/gold",
        ),
        execution_timeout=task_timeout(),
    )

    validate = BashOperator(
        task_id="validate_qttg",
        bash_command=spark_submit(
            "validate_qttg.py",
            f"--lake-dir {LAKE_DIR}",
        ),
        execution_timeout=task_timeout(30),
    )

    bronze >> silver >> gold >> validate
