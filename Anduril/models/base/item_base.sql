SELECT
    id AS item_id,
    name AS item
FROM {{ source('Anduril', 'item') }}
