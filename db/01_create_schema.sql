CREATE SCHEMA IF NOT EXISTS obrail;

-- =========================================================
-- ETL RUN LOG (traçabilité)
-- =========================================================
CREATE TABLE IF NOT EXISTS obrail.etl_run (
  run_id           BIGSERIAL PRIMARY KEY,
  pipeline_name    TEXT NOT NULL DEFAULT 'etl_gtfs_to_postgres',
  started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,
  status           TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING / SUCCESS / FAILED
  rows_extracted   INT DEFAULT 0,
  rows_loaded      INT DEFAULT 0,
  error_message    TEXT
);

-- =========================================================
-- TABLE FINALE : 1 ligne = 1 arrêt d’un trip (horaires)
-- (parfait pour filtres, KPI, dashboard)
-- =========================================================
CREATE TABLE IF NOT EXISTS obrail.fact_train_stop (
  -- Identifiants
  train_id         TEXT NOT NULL,         -- harmonisé (trip_id ou train_code)
  source           TEXT NOT NULL,         -- SNCF_France / BackOnTrack / GTFS_Europe
  category         TEXT NOT NULL,         -- jour / nuit
  operator         TEXT,                  -- SNCF / autre si dispo

  -- Arrêt / horaires
  stop_sequence    INT,                   -- ordre dans le trajet (GTFS)
  stop_name        TEXT,                  -- nom de l’arrêt
  arrival_time     TEXT,                  -- texte car peut dépasser 24:00:00
  departure_time   TEXT,

  -- Champs optionnels (BackOnTrack)
  route            TEXT,
  details          TEXT,
  tickets_url      TEXT,
  countries        TEXT,

  source_run_id    BIGINT NOT NULL REFERENCES obrail.etl_run(run_id),
  loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Empêche les doublons
  CONSTRAINT pk_fact_train_stop PRIMARY KEY (train_id, source, stop_sequence, stop_name)
);

CREATE INDEX IF NOT EXISTS idx_stop_name ON obrail.fact_train_stop(stop_name);
CREATE INDEX IF NOT EXISTS idx_source ON obrail.fact_train_stop(source);
CREATE INDEX IF NOT EXISTS idx_category ON obrail.fact_train_stop(category);
CREATE INDEX IF NOT EXISTS idx_train_id ON obrail.fact_train_stop(train_id);