WITH source AS (
    SELECT
        session_key,
        meeting_key,
        country_name,
        session_name,
        session_type,
        year,
        circuit_short_name,
        date_start,
        date_end
    FROM {{ source('F1_API', 'F1_SESSIONS') }}
)
SELECT * FROM source