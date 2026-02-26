-- =========================================================
-- KPI de base
-- =========================================================
SELECT source, category, COUNT(*) AS nb_rows
FROM obrail.fact_train_stop
GROUP BY source, category
ORDER BY source, category;

-- Top arrêts (tous)
SELECT stop_name, COUNT(*) AS nb
FROM obrail.fact_train_stop
WHERE stop_name IS NOT NULL
GROUP BY stop_name
ORDER BY nb DESC
LIMIT 20;

-- Exemple : rechercher un arrêt
SELECT *
FROM obrail.fact_train_stop
WHERE stop_name ILIKE '%Paris%'
LIMIT 50;

-- =========================================================
-- VUE : 1 ligne = 1 train (origine/destination + horaires)
-- origine = stop_sequence min, destination = stop_sequence max
-- =========================================================
CREATE OR REPLACE VIEW obrail.v_train_summary AS
WITH base AS (
  SELECT
    train_id, source, category,
    operator,
    stop_sequence, stop_name,
    arrival_time, departure_time
  FROM obrail.fact_train_stop
  WHERE stop_sequence IS NOT NULL
),
bounds AS (
  SELECT
    train_id, source,
    MIN(stop_sequence) AS min_seq,
    MAX(stop_sequence) AS max_seq
  FROM base
  GROUP BY train_id, source
)
SELECT
  b.train_id,
  b.source,
  b.category,
  COALESCE(b.operator, 'UNKNOWN') AS operator,

  o.stop_name AS origin_stop,
  o.departure_time AS origin_departure,

  d.stop_name AS destination_stop,
  d.arrival_time AS destination_arrival

FROM bounds b
LEFT JOIN base o
  ON o.train_id=b.train_id AND o.source=b.source AND o.stop_sequence=b.min_seq
LEFT JOIN base d
  ON d.train_id=b.train_id AND d.source=b.source AND d.stop_sequence=b.max_seq;

-- Vérifier la vue
SELECT * FROM obrail.v_train_summary LIMIT 50;

-- Top origines
SELECT origin_stop, COUNT(*) nb
FROM obrail.v_train_summary
WHERE origin_stop IS NOT NULL
GROUP BY origin_stop
ORDER BY nb DESC
LIMIT 20;

-- Top destinations
SELECT destination_stop, COUNT(*) nb
FROM obrail.v_train_summary
WHERE destination_stop IS NOT NULL
GROUP BY destination_stop
ORDER BY nb DESC
LIMIT 20;