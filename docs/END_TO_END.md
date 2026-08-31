# End-to-end check

The end-to-end runner sends one controlled Toronto weather event through the
local Kafka broker, persists it in PostgreSQL, starts a real Airflow DagRun,
and checks the generated Excel workbook against the database result.

It uses January 15, 2099 as an isolated processing date. Each invocation gets
its own Kafka topic, consumer group, Airflow run ID, and source marker, so the
check can be repeated without deleting local data. Repeated runs add another
identical observation to that date; aggregation remains deterministic.

## Run it

From the repository root:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
cp .env.example .env
.venv/bin/python scripts/run_e2e.py
```

Docker must be running. The script runs `docker compose up -d --wait`, safely
reseeds the development baselines, and unpauses `extreme_climate_pipeline`.
Environment variables can override the local defaults in `.env.example`; if
you change them in `.env`, export the same values before running the script.

## Expected result

The command exits zero and prints one JSON document with `"status": "passed"`.
The exact IDs, Kafka topic, offset, and observation count change between runs.
The stable checks are:

- one event is acknowledged by Kafka and stored with its topic, partition, and
  offset in `raw_weather`;
- the Toronto `2099-01-15` daily summary exists and is labelled `anomaly`;
- all four Airflow task states are `success`;
- `reports/extreme_climate_daily_2099-01-15.xlsx` contains the same summary
  values as PostgreSQL and includes the temperature chart.

The generated workbook is ignored by Git. The Compose services are left
running for inspection with `docker compose ps`; stop them with
`docker compose down` when finished.

## Verified checkpoint

This workflow was run successfully on August 31, 2026. The controlled event
was delivered to Kafka partition 0 at offset 0, persisted as `raw_weather_id`
5, and aggregated into one Toronto row with a mean temperature of 50 °C and
25 mm total precipitation. The anomaly status was `anomaly`; every Airflow
task finished in `success`; and the workbook row matched those database values.

IDs and counts above describe that run only. The runner deliberately checks
the relationships and values rather than expecting those generated identifiers
to remain fixed.
