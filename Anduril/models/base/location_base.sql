SELECT
    id AS location_id,
    name AS location
FROM {{ source('Anduril', 'location') }}
