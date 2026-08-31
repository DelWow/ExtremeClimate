"""Import-only smoke DAG for validating the local Airflow deployment."""

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="extreme_climate_smoke",
    description="Verify that the Extreme Climate DAG directory loads.",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["extreme-climate", "smoke"],
) as dag:
    EmptyOperator(task_id="smoke")
