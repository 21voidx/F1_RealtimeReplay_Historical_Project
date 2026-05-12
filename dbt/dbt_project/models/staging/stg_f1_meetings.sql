WITH source AS (
    SELECT
        meeting_key,
        meeting_name,
        meeting_official_name,
        country_name,
        circuit_short_name,
        year,
        date_start
    FROM {{ source('F1_API', 'F1_MEETINGS') }}
)
SELECT * FROM source