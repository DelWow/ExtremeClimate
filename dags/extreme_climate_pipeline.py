"""Daily Airflow workflow for the Extreme Climate reporting pipeline."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from extreme_climate.pipeline_tasks import (
    detect_anomalies_task,
    generate_report_task,
    transform_weather_task,
    validate_weather_task,
)


DATE_WINDOW = {
    "window_start": "{{ data_interval_start | ds }}",
    "window_end": "{{ data_interval_end | ds }}",
}


with DAG(
    dag_id="extreme_climate_pipeline",
    description="Validate, aggregate, evaluate, and report daily weather data.",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "extreme-climate",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["extreme-climate", "weather"],
) as dag:
    validate = PythonOperator(
        task_id="validate_raw_weather",
        python_callable=validate_weather_task,
        op_kwargs=DATE_WINDOW,
    )
    transform = PythonOperator(
        task_id="transform_daily_weather",
        python_callable=transform_weather_task,
        op_kwargs=DATE_WINDOW,
    )
    detect = PythonOperator(
        task_id="detect_weather_anomalies",
        python_callable=detect_anomalies_task,
        op_kwargs=DATE_WINDOW,
    )
    report = PythonOperator(
        task_id="generate_excel_report",
        python_callable=generate_report_task,
        op_kwargs=DATE_WINDOW,
    )

    validate >> transform >> detect >> report
