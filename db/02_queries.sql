-- Jour vs nuit
SELECT is_night_train, COUNT(*) AS nb_trips
FROM obrail.fact_trip_summary
GROUP BY is_night_train
ORDER BY is_night_train;

-- Top gares de départ
SELECT dep_stop_name, COUNT(*) AS nb
FROM obrail.fact_trip_summary
GROUP BY dep_stop_name
ORDER BY nb DESC
LIMIT 20;

-- Top gares d'arrivée
SELECT arr_stop_name, COUNT(*) AS nb
FROM obrail.fact_trip_summary
GROUP BY arr_stop_name
ORDER BY nb DESC
LIMIT 20;

-- Ex : filtrer des trajets (utile pour future API)
SELECT *
FROM obrail.fact_trip_summary
WHERE dep_stop_name ILIKE '%Paris%'
  AND arr_stop_name ILIKE '%Lyon%'
LIMIT 50;
