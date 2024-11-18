SELECT
    date as effective_date,
    item_id,
    location_id,
    cost
FROM {{ source('Anduril', 'costs') }}
