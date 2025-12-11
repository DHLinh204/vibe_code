
WITH raw_reverse_geo AS (
  SELECT
    *
  FROM
    {{ ref('reverse_geocode') }}
)

SELECT
    osm_id,
    street_name,
    ward
FROM raw_reverse_geo
