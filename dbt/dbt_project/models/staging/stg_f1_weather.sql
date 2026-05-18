SELECT *
FROM {{ source('F1_API', 'RAW_WEATHER') }}