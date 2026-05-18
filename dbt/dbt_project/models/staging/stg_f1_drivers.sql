WITH source AS (
    SELECT DISTINCT
        driver_number,
        broadcast_name,
        first_name,
        last_name,
        full_name,
        name_acronym,
        team_name,
        team_colour,
        country_code,
        session_key
    FROM {{ source('F1_API', 'RAW_DRIVERS') }}
)
SELECT * FROM source