BEGIN;

CREATE TABLE IF NOT EXISTS raw_weather (
    raw_weather_id BIGINT GENERATED ALWAYS AS IDENTITY,
    region_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature_c NUMERIC(5, 2) NOT NULL,
    humidity_percent NUMERIC(5, 2),
    precipitation_mm NUMERIC(10, 2),
    wind_speed_mps NUMERIC(8, 2),
    source_payload JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT raw_weather_pkey PRIMARY KEY (raw_weather_id),
    CONSTRAINT raw_weather_region_id_check CHECK (
        region_id = BTRIM(region_id)
        AND region_id <> ''
        AND CHAR_LENGTH(region_id) <= 64
    ),
    CONSTRAINT raw_weather_temperature_c_check CHECK (
        temperature_c <> 'NaN'::NUMERIC
        AND temperature_c >= -273.15
    ),
    CONSTRAINT raw_weather_humidity_percent_check CHECK (
        humidity_percent IS NULL
        OR (humidity_percent <> 'NaN'::NUMERIC
            AND humidity_percent BETWEEN 0 AND 100)
    ),
    CONSTRAINT raw_weather_precipitation_mm_check CHECK (
        precipitation_mm IS NULL
        OR (precipitation_mm <> 'NaN'::NUMERIC
            AND precipitation_mm >= 0)
    ),
    CONSTRAINT raw_weather_wind_speed_mps_check CHECK (
        wind_speed_mps IS NULL
        OR (wind_speed_mps <> 'NaN'::NUMERIC
            AND wind_speed_mps >= 0)
    ),
    CONSTRAINT raw_weather_source_payload_check CHECK (
        source_payload IS NULL
        OR JSONB_TYPEOF(source_payload) = 'object'
    )
);

-- Kafka positions make database writes idempotent when a message is replayed
-- after its database transaction commits but before its offset is committed.
ALTER TABLE raw_weather
    ADD COLUMN IF NOT EXISTS kafka_topic TEXT,
    ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,
    ADD COLUMN IF NOT EXISTS kafka_offset BIGINT;

DO $schema$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'raw_weather'::REGCLASS
          AND conname = 'raw_weather_kafka_position_check'
    ) THEN
        ALTER TABLE raw_weather
            ADD CONSTRAINT raw_weather_kafka_position_check CHECK (
                (kafka_topic IS NULL
                    AND kafka_partition IS NULL
                    AND kafka_offset IS NULL)
                OR (kafka_topic IS NOT NULL
                    AND kafka_topic = BTRIM(kafka_topic)
                    AND kafka_topic <> ''
                    AND CHAR_LENGTH(kafka_topic) <= 249
                    AND kafka_partition IS NOT NULL
                    AND kafka_partition >= 0
                    AND kafka_offset IS NOT NULL
                    AND kafka_offset >= 0)
            );
    END IF;
END
$schema$;

CREATE UNIQUE INDEX IF NOT EXISTS raw_weather_kafka_position_uidx
    ON raw_weather (kafka_topic, kafka_partition, kafka_offset);

-- Supports per-region time-window reads by validation and aggregation jobs.
CREATE INDEX IF NOT EXISTS raw_weather_region_observed_at_idx
    ON raw_weather (region_id, observed_at DESC);

-- Supports time-window reads across all configured regions.
CREATE INDEX IF NOT EXISTS raw_weather_observed_at_idx
    ON raw_weather (observed_at DESC);

CREATE TABLE IF NOT EXISTS historical_baselines (
    region_id TEXT NOT NULL,
    baseline_month SMALLINT NOT NULL,
    baseline_day SMALLINT NOT NULL,
    mean_temperature_c NUMERIC(5, 2),
    mean_humidity_percent NUMERIC(5, 2),
    mean_precipitation_mm NUMERIC(10, 2),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT historical_baselines_pkey PRIMARY KEY (
        region_id,
        baseline_month,
        baseline_day
    ),
    CONSTRAINT historical_baselines_region_id_check CHECK (
        region_id = BTRIM(region_id)
        AND region_id <> ''
        AND CHAR_LENGTH(region_id) <= 64
    ),
    CONSTRAINT historical_baselines_month_check CHECK (
        baseline_month BETWEEN 1 AND 12
    ),
    CONSTRAINT historical_baselines_day_check CHECK (
        baseline_day BETWEEN 1 AND CASE baseline_month
            WHEN 2 THEN 29
            WHEN 4 THEN 30
            WHEN 6 THEN 30
            WHEN 9 THEN 30
            WHEN 11 THEN 30
            ELSE 31
        END
    ),
    CONSTRAINT historical_baselines_temperature_c_check CHECK (
        mean_temperature_c IS NULL
        OR (mean_temperature_c <> 'NaN'::NUMERIC
            AND mean_temperature_c >= -273.15)
    ),
    CONSTRAINT historical_baselines_humidity_percent_check CHECK (
        mean_humidity_percent IS NULL
        OR (mean_humidity_percent <> 'NaN'::NUMERIC
            AND mean_humidity_percent BETWEEN 0 AND 100)
    ),
    CONSTRAINT historical_baselines_precipitation_mm_check CHECK (
        mean_precipitation_mm IS NULL
        OR (mean_precipitation_mm <> 'NaN'::NUMERIC
            AND mean_precipitation_mm >= 0)
    ),
    CONSTRAINT historical_baselines_measure_check CHECK (
        NUM_NONNULLS(
            mean_temperature_c,
            mean_humidity_percent,
            mean_precipitation_mm
        ) > 0
    )
);

-- Supports calendar-day lookups across all regions during baseline loading.
CREATE INDEX IF NOT EXISTS historical_baselines_calendar_day_idx
    ON historical_baselines (baseline_month, baseline_day);

CREATE TABLE IF NOT EXISTS weather_daily_summary (
    region_id TEXT NOT NULL,
    summary_date DATE NOT NULL,
    observation_count INTEGER NOT NULL,
    mean_temperature_c NUMERIC(5, 2),
    min_temperature_c NUMERIC(5, 2),
    max_temperature_c NUMERIC(5, 2),
    mean_humidity_percent NUMERIC(5, 2),
    total_precipitation_mm NUMERIC(12, 2),
    max_wind_speed_mps NUMERIC(8, 2),
    is_anomaly BOOLEAN,
    anomaly_details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT weather_daily_summary_pkey PRIMARY KEY (
        region_id,
        summary_date
    ),
    CONSTRAINT weather_daily_summary_region_id_check CHECK (
        region_id = BTRIM(region_id)
        AND region_id <> ''
        AND CHAR_LENGTH(region_id) <= 64
    ),
    CONSTRAINT weather_daily_summary_observation_count_check CHECK (
        observation_count > 0
    ),
    CONSTRAINT weather_daily_summary_temperature_check CHECK (
        (mean_temperature_c IS NULL
            AND min_temperature_c IS NULL
            AND max_temperature_c IS NULL)
        OR (mean_temperature_c IS NOT NULL
            AND min_temperature_c IS NOT NULL
            AND max_temperature_c IS NOT NULL
            AND mean_temperature_c <> 'NaN'::NUMERIC
            AND min_temperature_c <> 'NaN'::NUMERIC
            AND max_temperature_c <> 'NaN'::NUMERIC
            AND min_temperature_c >= -273.15
            AND min_temperature_c <= mean_temperature_c
            AND mean_temperature_c <= max_temperature_c)
    ),
    CONSTRAINT weather_daily_summary_humidity_percent_check CHECK (
        mean_humidity_percent IS NULL
        OR (mean_humidity_percent <> 'NaN'::NUMERIC
            AND mean_humidity_percent BETWEEN 0 AND 100)
    ),
    CONSTRAINT weather_daily_summary_precipitation_mm_check CHECK (
        total_precipitation_mm IS NULL
        OR (total_precipitation_mm <> 'NaN'::NUMERIC
            AND total_precipitation_mm >= 0)
    ),
    CONSTRAINT weather_daily_summary_wind_speed_mps_check CHECK (
        max_wind_speed_mps IS NULL
        OR (max_wind_speed_mps <> 'NaN'::NUMERIC
            AND max_wind_speed_mps >= 0)
    ),
    CONSTRAINT weather_daily_summary_measure_check CHECK (
        NUM_NONNULLS(
            mean_temperature_c,
            mean_humidity_percent,
            total_precipitation_mm,
            max_wind_speed_mps
        ) > 0
    ),
    CONSTRAINT weather_daily_summary_anomaly_details_check CHECK (
        anomaly_details IS NULL
        OR JSONB_TYPEOF(anomaly_details) = 'object'
    )
);

-- Supports date-range reporting across all regions.
CREATE INDEX IF NOT EXISTS weather_daily_summary_date_idx
    ON weather_daily_summary (summary_date DESC);

COMMENT ON TABLE raw_weather IS
    'Normalized observations received from the weather event stream.';
COMMENT ON COLUMN raw_weather.kafka_offset IS
    'Kafka source position; unique with topic and partition for replay safety.';
COMMENT ON TABLE historical_baselines IS
    'Daily climatological means keyed by region and calendar month/day.';
COMMENT ON TABLE weather_daily_summary IS
    'One aggregate row per region and calendar date.';
COMMENT ON COLUMN weather_daily_summary.is_anomaly IS
    'NULL until anomaly evaluation; TRUE or FALSE after evaluation.';

COMMIT;
