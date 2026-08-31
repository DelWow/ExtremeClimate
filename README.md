# Extreme Climate

Extreme Climate is a local weather data pipeline built around Kafka,
PostgreSQL, Airflow, and Excel. It collects current readings for five Canadian
cities, keeps the original observations, builds daily summaries, compares them
with a development climate baseline, and writes a report with anomaly
highlighting and a temperature chart.

This is a development project, not a forecasting or alerting service. The
historical baseline is an illustrative fixture and should not be used for
scientific or safety decisions.

## How it fits together

```text
Open-Meteo -> Python producer -> Kafka -> Python consumer -> PostgreSQL
                                                            |
                                                            v
Airflow: validate -> daily summary -> anomaly check -> Excel report
```

The producer and consumer run separately from the Airflow DAG. Airflow starts
once observations are already in `raw_weather` and handles the daily database
work and report generation.

The main tables are:

- `raw_weather`: normalized observations plus their Kafka positions;
- `historical_baselines`: calendar-day comparison values;
- `weather_daily_summary`: daily aggregates and anomaly results.

## What you need

- Python 3.9 or newer
- Docker with Compose v2
- enough Docker memory for PostgreSQL, Kafka, and the two Airflow services
- internet access on the first start to pull images and Python packages

The defaults bind PostgreSQL to `127.0.0.1:5433`, Kafka to
`127.0.0.1:29092`, and Airflow to `http://127.0.0.1:8080`. Change the matching
values in `.env` if those ports are already in use.

## Set up the project

Run these commands from the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
export PYTHONPATH="$PWD/src"
docker compose up -d --wait
```

The values in `.env.example` are local-only defaults. Docker Compose reads
`.env` automatically. The Python commands use the same built-in defaults; if
you customize `.env`, export those values in your shell before running a Python
command:

```sh
set -a
. ./.env
set +a
```

Check the services with:

```sh
docker compose ps
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

The second command should print `No data found`.

## Run a verified pipeline cycle

The quickest way to exercise the whole project is the checked-in end-to-end
runner:

```sh
.venv/bin/python scripts/run_e2e.py
```

It starts the Compose stack if needed, seeds the baseline fixture, publishes a
controlled Toronto event to a unique Kafka topic, consumes it into PostgreSQL,
triggers a real Airflow run, and checks the workbook against the database. A
successful run exits with code zero and prints JSON containing:

```json
{
  "status": "passed"
}
```

The complete output also includes the Kafka position, database result, Airflow
task states, and verified report row. The report is written to
`reports/extreme_climate_daily_2099-01-15.xlsx`. See
[docs/END_TO_END.md](docs/END_TO_END.md) for the fixed test data and expected
checks.

## Run the pieces by hand

Seed the development baseline first. The command is safe to repeat and should
finish with 1,830 stored rows:

```sh
.venv/bin/python -m extreme_climate.baseline_seed
```

Start the consumer in one terminal:

```sh
export PYTHONPATH="$PWD/src"
.venv/bin/python -m extreme_climate.weather_consumer
```

In another terminal, fetch the current readings and publish them to Kafka:

```sh
export PYTHONPATH="$PWD/src"
.venv/bin/python -m extreme_climate.weather_producer
```

The producer prints one normalized JSON event per configured city. The
consumer prints the Kafka position after each event is committed to PostgreSQL.
Use `--print-only` on the producer to inspect provider data without publishing,
or `--max-messages 5` on the consumer when you want it to exit on its own.

Open `http://127.0.0.1:8080` and sign in with the local credentials from
`.env` (`airflow` / `airflow_dev` by default). The
`extreme_climate_pipeline` DAG runs daily and is created paused. For a manual
run, unpause it and supply a half-open date window in the trigger configuration:

```json
{
  "window_start": "2026-08-31",
  "window_end": "2026-09-01"
}
```

That run creates `reports/extreme_climate_daily_2026-08-31.xlsx`. Re-running
the same date is safe: summary rows are upserted, anomaly labels are refreshed,
and the report is replaced.

## Configuration

Most local settings live in `.env`:

| Setting | Purpose |
| --- | --- |
| `POSTGRES_*` | Application database connection and host port |
| `KAFKA_*` | Broker address, topic, clients, and consumer group |
| `WEATHER_API_URL` | Current-weather provider endpoint |
| `REGIONS_CONFIG_PATH` | Region and timezone configuration |
| `AIRFLOW_*` | Image, UI port, metadata database, and local login |
| `REPORT_OUTPUT_DIR` | Report path inside the Airflow containers |

Regions are defined in [config/regions.yaml](config/regions.yaml). The
development baseline and its assumptions are documented in
[data/HISTORICAL_BASELINES.md](data/HISTORICAL_BASELINES.md).

Do not reuse the checked-in passwords outside local development. Kafka uses
plaintext connections by default; the optional SASL and TLS variables in
`.env.example` are there for environments that provide an authenticated
broker.

## Tests

Run the unit and DAG-structure tests with:

```sh
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/pytest -q
```

Validate Compose and run the live integration check with:

```sh
docker compose config -q
.venv/bin/python scripts/run_e2e.py
```

Generated reports, logs, local environment files, and caches are ignored by
Git. Docker keeps the database volumes outside the working tree.

## If something goes wrong

- If a Python module cannot be found, run `export PYTHONPATH="$PWD/src"` from
  the repository root.
- If a service never becomes healthy, start with `docker compose ps` and then
  inspect it with `docker compose logs <service-name>`.
- Airflow's first start is slower because its local containers install the
  pinned report and database libraries before starting.
- A consumer that prints nothing is waiting for a Kafka event. Start the
  producer, and make sure both processes use the same `KAFKA_TOPIC`.
- An empty report usually means there were no raw observations in the selected
  region-local date window. Check the trigger dates and consumer output.
- Python 3.9 builds linked against LibreSSL may print an `urllib3` compatibility
  warning. It does not affect the local pipeline tests, but a newer Python from
  python.org or Homebrew removes the warning.

Airflow task logs are also available in the UI and under `logs/`.

## Stop or reset the stack

Stop the containers while keeping both databases:

```sh
docker compose down
```

To start over completely, remove the containers and named volumes:

```sh
docker compose down --volumes
```

The second command permanently deletes the local application data, Airflow
history, and seeded baselines. Generated workbooks under `reports/` are regular
host files and are not removed by either command.

The remaining build checklist is in [TODO.md](TODO.md).
