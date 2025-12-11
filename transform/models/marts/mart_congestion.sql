with base as (
    select
        f.osm_id,
        f.street_name,
        f.ward,
        f.district,
        f.city,
        f.lat,
        f.lon,
        f.current_speed,
        f.free_flow_speed,
        -- timestamp / hour (populated from ingest)
        f.observed_at::timestamp as observed_at,
        date_trunc('hour', f.observed_at::timestamp) as observed_hour,
        cast(date_part('hour', f.observed_at::timestamp) as integer) as hour_of_day,
        -- computed metrics
        case
          when f.free_flow_speed is not null and f.free_flow_speed > 0
          then (f.current_speed::double precision / f.free_flow_speed::double precision)
          else null
        end as speed_ratio,
        case
          when f.free_flow_speed is not null and f.free_flow_speed > 0
               and (f.current_speed::double precision / f.free_flow_speed::double precision) < 0.7
          then true
          else false
        end as is_congested,
        -- join weather (attempt to match on same hour)
        w.weather_main,
        w.weather_description,
        w.temp,
        w.humidity,
        case
          when lower(w.weather_main) like '%rain%' or lower(w.weather_description) like '%rain%' then true
          else false
        end as is_raining
    from {{ ref('fact_traffic') }} f
    left join {{ ref('fact_weather') }} w
      on f.osm_id = w.osm_id
      and date_trunc('hour', f.observed_at::timestamp) = date_trunc('hour', w.observed_at::timestamp)
)

select *
from base
