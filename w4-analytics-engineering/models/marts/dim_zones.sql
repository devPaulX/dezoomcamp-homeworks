WITH taxi_zone_lookup AS (
    SELECT
        *
    FROM
        {{ source('zoomcamp','taxi_zones') }}
),
renamed as (
    SELECT
        LocationID AS location_id,
        borough,
        zone,
        service_zone
    FROM
        taxi_zone_lookup
)

select * from renamed