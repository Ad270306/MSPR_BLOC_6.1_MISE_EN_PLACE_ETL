CREATE SCHEMA IF NOT EXISTS obrail;

-- Log d'exécution ETL (traçabilité)
CREATE TABLE IF NOT EXISTS obrail.etl_run (
  run_id           BIGSERIAL PRIMARY KEY,
  source_name      TEXT NOT NULL,
  source_url       TEXT NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  status           TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING / SUCCESS / FAILED
  rows_extracted   INT DEFAULT 0,
  rows_loaded      INT DEFAULT 0,
  error_message    TEXT
);

-- Table analytique : 1 ligne = 1 trip (départ/arrivée)
CREATE TABLE IF NOT EXISTS obrail.fact_trip_summary (
  trip_id               TEXT PRIMARY KEY,
  source_name           TEXT NOT NULL,

  dep_stop_name         TEXT NOT NULL,
  arr_stop_name         TEXT NOT NULL,

  departure_time        TEXT NOT NULL, -- GTFS peut dépasser 24:00:00
  arrival_time          TEXT NOT NULL,

  is_night_train        BOOLEAN NOT NULL,
  duration_seconds_est  INT,

  source_run_id         BIGINT NOT NULL REFERENCES obrail.etl_run(run_id),
  loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_dep_name ON obrail.fact_trip_summary(dep_stop_name);
CREATE INDEX IF NOT EXISTS idx_fact_arr_name ON obrail.fact_trip_summary(arr_stop_name);
CREATE INDEX IF NOT EXISTS idx_fact_is_night ON obrail.fact_trip_summary(is_night_train);
