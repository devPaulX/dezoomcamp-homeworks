WITH source AS (
    SELECT * 
    FROM {{ source('zoomcamp', 'fhv_tripdata') }}
),

renamed AS (
    SELECT
        -- timestamps
        CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- identifiers
        CAST(PULocationID AS INT64) AS pickup_location_id,
        CAST(DOLocationID AS INT64) AS dropoff_location_id,
        
        -- base numbers and flags
        CAST(dispatching_base_num AS STRING) AS dispatching_base_num,
        {{ safe_cast('SR_Flag', 'INT64') }} AS sr_flag,
        CAST(affiliated_base_number AS STRING) AS affiliated_base_number,

        -- service type
        'FHV' AS service_type
    FROM source
)

SELECT *
FROM renamed
WHERE dispatching_base_num IS NOT NULL

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
  AND pickup_datetime >= '2019-01-01' 
  AND pickup_datetime < '2019-02-01'
{% endif %}
