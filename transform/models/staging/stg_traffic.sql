

WITH raw_traffic AS (
  SELECT
    *
  FROM
    raw.tom_tom_traffic
)

SELECT
    osm_id,
    lat,
    lon,
    current_speed,
    free_flow_speed,
  -- ingest-provided event timestamp
  observed_at,
    confidence,
    road_closure
FROM raw_traffic
