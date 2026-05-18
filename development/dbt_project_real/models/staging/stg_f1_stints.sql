SELECT
    *
FROM {{ source('F1_API', 'RAW_STINTS') }}
WHERE lap_start IS NOT NULL
  AND lap_end IS NOT NULL