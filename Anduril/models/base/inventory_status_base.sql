SELECT
    id AS inventory_status_id,
    name AS status
FROM {{ source('Anduril', 'inventory_status') }}
