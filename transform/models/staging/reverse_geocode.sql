
WITH raw_reverse_geo AS (
  SELECT
    *
  FROM
    raw.tom_tom_reverse_geocode
)

SELECT
    osm_id,
    input_lat,
    input_lon,
  -- ingest-provided event timestamp
  observed_at,
    street_name,
    ward,
    district,
    city
FROM raw_reverse_geo
