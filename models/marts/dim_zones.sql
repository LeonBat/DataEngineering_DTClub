select
    locationid as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxizones_lookup') }}