WITH source AS (
    SELECT * FROM {{ source('HITESH', 'F1_POSITION') }}
)
SELECT
    session_key,
    driver_number,
    position,
    date
FROM source