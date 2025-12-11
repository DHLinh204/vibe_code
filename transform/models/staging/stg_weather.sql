
WITH raw_weather AS (
  SELECT
    *
  FROM
    raw.open_weather_current
)

SELECT
    osm_id,
    lat,
    lon,
  -- ingest-provided event timestamp
  observed_at,
    weather_main,
    weather_description,
    temp,
    feels_like,
    temp_min,
    temp_max,
    humidity,
    city_name
FROM raw_weather

