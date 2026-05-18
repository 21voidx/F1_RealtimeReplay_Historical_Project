WITH source AS (
    SELECT * FROM {{ source('F1_API', 'RAW_POSITION') }}
)
SELECT
    session_key,
    driver_number,
    position,
    date
FROM source