WITH source AS (
    SELECT * FROM {{ source('F1_API', 'F1_POSITION') }}
)
SELECT
    session_key,
    driver_number,
    position,
    date
FROM source