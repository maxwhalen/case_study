WITH enriched_data AS (
    SELECT
        btl.transaction_date AS date,
        l.location,
        b.bin,
        s.status,
        i.item,
        btl.quantity,
        c.cost,
        (btl.quantity * c.cost) AS value
    FROM {{ ref('transaction_line_base') }} btl
    LEFT JOIN {{ ref('item_base') }} i ON btl.item_id = i.item_id
    LEFT JOIN {{ ref('location_base') }} l ON btl.location_id = l.location_id
    LEFT JOIN {{ ref('bin_base') }} b ON btl.bin_id = b.bin_id
    LEFT JOIN {{ ref('inventory_status_base') }} s ON btl.inventory_status_id = s.inventory_status_id
    LEFT JOIN {{ ref('costs_base') }} c
        ON btl.item_id = c.item_id
        AND btl.location_id = c.location_id
        AND btl.transaction_date >= c.effective_date
)
SELECT
    date,
    location,
    bin,
    status,
    item,
    SUM(quantity) AS quantity,
    SUM(value) AS value
FROM enriched_data
GROUP BY date, location, bin, status, item
