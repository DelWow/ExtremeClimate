import runpy
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = PROJECT_ROOT / "docker-compose.yml"
PIPELINE_DAG_PATH = PROJECT_ROOT / "dags" / "extreme_climate_pipeline.py"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


def test_compose_adds_only_minimum_local_executor_airflow_services() -> None:
    services = _compose()["services"]

    assert {
        "postgres",
        "kafka",
        "airflow-postgres",
        "airflow-init",
        "airflow-webserver",
        "airflow-scheduler",
    } == set(services)
    assert services["airflow-webserver"]["image"] == (
        "${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.5}"
    )
    assert services["airflow-scheduler"]["environment"][
        "AIRFLOW__CORE__EXECUTOR"
    ] == "LocalExecutor"
    assert "airflow-worker" not in services
    assert "redis" not in services
    assert "triggerer" not in services


def test_airflow_metadata_database_is_private_and_persistent() -> None:
    compose = _compose()
    database = compose["services"]["airflow-postgres"]
    scheduler = compose["services"]["airflow-scheduler"]

    assert database["image"] == "postgres:16-alpine"
    assert "ports" not in database
    assert database["volumes"] == [
        "airflow_postgres_data:/var/lib/postgresql/data"
    ]
    assert database["healthcheck"]["test"] == [
        "CMD-SHELL",
        'pg_isready -U "$${POSTGRES_USER}" -d "$${POSTGRES_DB}"',
    ]
    assert "airflow_postgres_data" in compose["volumes"]
    assert scheduler["environment"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == (
        "${AIRFLOW_DATABASE_URL:-postgresql+psycopg2://airflow:"
        "airflow_dev@airflow-postgres/airflow}"
    )


def test_airflow_initialization_migrates_database_and_creates_admin() -> None:
    init = _compose()["services"]["airflow-init"]

    assert init["command"] == "version"
    assert init["restart"] == "no"
    assert init["environment"]["_AIRFLOW_DB_MIGRATE"] == "true"
    assert init["environment"]["_AIRFLOW_WWW_USER_CREATE"] == "true"
    assert init["environment"]["_AIRFLOW_WWW_USER_USERNAME"] == (
        "${AIRFLOW_ADMIN_USERNAME:-airflow}"
    )
    assert init["depends_on"]["airflow-postgres"]["condition"] == (
        "service_healthy"
    )


def test_airflow_runtime_services_wait_for_successful_initialization() -> None:
    services = _compose()["services"]

    for service_name in ("airflow-webserver", "airflow-scheduler"):
        service = services[service_name]
        assert service["depends_on"]["airflow-postgres"]["condition"] == (
            "service_healthy"
        )
        assert service["depends_on"]["airflow-init"]["condition"] == (
            "service_completed_successfully"
        )
        assert service["depends_on"]["postgres"]["condition"] == (
            "service_healthy"
        )
        assert service["restart"] == "unless-stopped"
        assert service["environment"]["AIRFLOW__CORE__LOAD_EXAMPLES"] == "false"


def test_airflow_services_mount_required_project_directories() -> None:
    scheduler = _compose()["services"]["airflow-scheduler"]

    assert scheduler["volumes"] == [
        "./dags:/opt/airflow/dags:ro",
        "./logs:/opt/airflow/logs",
        "./src:/opt/airflow/project/src:ro",
        "./config:/opt/airflow/project/config:ro",
        "./data:/opt/airflow/project/data:ro",
        "./reports:/opt/airflow/project/reports",
    ]
    assert scheduler["environment"]["PYTHONPATH"] == "/opt/airflow/project/src"


def test_airflow_tasks_have_application_database_and_runtime_dependencies() -> None:
    environment = _compose()["services"]["airflow-scheduler"]["environment"]

    assert environment["POSTGRES_HOST"] == "postgres"
    assert environment["POSTGRES_PORT"] == 5432
    assert environment["REGIONS_CONFIG_PATH"] == (
        "/opt/airflow/project/config/regions.yaml"
    )
    assert environment["REPORT_OUTPUT_DIR"] == "/opt/airflow/project/reports"
    assert environment["_PIP_ADDITIONAL_REQUIREMENTS"] == (
        "psycopg[binary]==3.2.13 openpyxl==3.1.5 PyYAML==6.0.3"
    )


def test_airflow_webserver_and_scheduler_have_component_healthchecks() -> None:
    services = _compose()["services"]

    assert services["airflow-webserver"]["healthcheck"]["test"] == [
        "CMD",
        "curl",
        "--fail",
        "http://localhost:8080/health",
    ]
    assert services["airflow-scheduler"]["environment"][
        "AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK"
    ] == "true"
    assert services["airflow-scheduler"]["healthcheck"]["test"] == [
        "CMD",
        "curl",
        "--fail",
        "http://localhost:8974/health",
    ]
    assert services["airflow-webserver"]["ports"] == [
        "127.0.0.1:${AIRFLOW_PORT:-8080}:8080"
    ]


def test_pipeline_dag_imports_with_ordered_task_structure(monkeypatch) -> None:
    dag_calls = []
    tasks = []
    dependencies = []

    class FakeDAG:
        def __init__(self, **kwargs):
            dag_calls.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class FakePythonOperator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            tasks.append(self)

        def __rshift__(self, other):
            dependencies.append(
                (self.kwargs["task_id"], other.kwargs["task_id"])
            )
            return other

    airflow_module = ModuleType("airflow")
    airflow_module.DAG = FakeDAG
    operators_module = ModuleType("airflow.operators")
    python_module = ModuleType("airflow.operators.python")
    python_module.PythonOperator = FakePythonOperator
    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python_module)

    namespace = runpy.run_path(str(PIPELINE_DAG_PATH))

    assert "dag" in namespace
    assert len(dag_calls) == 1
    dag_config = dag_calls[0]
    assert dag_config["dag_id"] == "extreme_climate_pipeline"
    assert dag_config["description"] == (
        "Validate, aggregate, evaluate, and report daily weather data."
    )
    assert dag_config["start_date"].isoformat() == "2024-01-01T00:00:00+00:00"
    assert dag_config["schedule"] == "@daily"
    assert dag_config["catchup"] is False
    assert dag_config["max_active_runs"] == 1
    assert dag_config["default_args"] == {
        "owner": "extreme-climate",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
    assert dag_config["tags"] == ["extreme-climate", "weather"]

    assert [task.kwargs["task_id"] for task in tasks] == [
        "validate_raw_weather",
        "transform_daily_weather",
        "detect_weather_anomalies",
        "generate_excel_report",
    ]
    assert [task.kwargs["python_callable"].__name__ for task in tasks] == [
        "validate_weather_task",
        "transform_weather_task",
        "detect_anomalies_task",
        "generate_report_task",
    ]
    assert all(
        task.kwargs["op_kwargs"]
        == {
            "window_start": "{{ data_interval_start | ds }}",
            "window_end": "{{ data_interval_end | ds }}",
        }
        for task in tasks
    )
    assert dependencies == [
        ("validate_raw_weather", "transform_daily_weather"),
        ("transform_daily_weather", "detect_weather_anomalies"),
        ("detect_weather_anomalies", "generate_excel_report"),
    ]
