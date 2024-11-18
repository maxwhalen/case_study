SELECT
    transaction_date,
    item_id,
    bin_id,
    inventory_status_id,
    location_id,
    quantity,
    -- Add a flag to indicate non-integer quantities
    CASE
        WHEN (quantity != FLOOR(quantity) or quantity != CEILING(quantity)) THEN 1
        ELSE 0
    END AS is_non_integer
FROM {{ source('Anduril', 'transaction_line') }}
