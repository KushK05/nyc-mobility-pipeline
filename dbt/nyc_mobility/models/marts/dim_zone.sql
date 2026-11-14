select
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone,
    (contains_substr(Zone, 'Airport') or LocationID in (1, 132, 138)) as is_airport
from {{ ref('taxi_zone_lookup') }}
