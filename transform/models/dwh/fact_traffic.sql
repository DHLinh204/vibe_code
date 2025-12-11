
select
    f.*,
    d.street_name,
    d.ward,
    d.district,
    d.city
from {{ ref('stg_traffic') }} f
left join {{ ref('reverse_geocode') }} d
    on f.osm_id = d.osm_id
