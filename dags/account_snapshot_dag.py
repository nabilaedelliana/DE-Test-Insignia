import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from q1_pipeline.account_snapshot import run_pipeline


logger = logging.getLogger(__name__)


def notify_pipeline_failure(context):
    """
    Failure notification callback.

    In production, this can be replaced with an integration
    to email, Slack, SNS, or another alerting service.
    """
    task_instance = context["task_instance"]

    logger.error(
        "ALERT: account snapshot pipeline failed after all retries. "
        "dag_id=%s task_id=%s run_id=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("run_id"),
    )


def run_account_snapshot():
    run_pipeline("2026-02-18")


def trigger_credit_scoring():
    """
    Downstream trigger placeholder.

    In production, this task would trigger the credit scoring
    job/workflow after the account snapshot succeeds.
    """
    logger.info(
        "Credit scoring trigger executed after successful account snapshot."
    )


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_pipeline_failure,
}


with DAG(
    dag_id="account_snapshot_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["q1", "data-engineering"],
) as dag:

    account_snapshot = PythonOperator(
        task_id="account_snapshot",
        python_callable=run_account_snapshot,
    )

    credit_scoring = PythonOperator(
        task_id="trigger_credit_scoring",
        python_callable=trigger_credit_scoring,
    )

    account_snapshot >> credit_scoring
