WITH enriched_data AS (
    SELECT
        tlb.transaction_line_id,
        tlb.transaction_date AS date,
        l.location,
        b.bin,
        s.status,
        i.item,
        tlb.quantity,
        c.cost,
        (tlb.quantity * c.cost) AS value,
        ROW_NUMBER() OVER (
            PARTITION BY tlb.transaction_line_id  
            ORDER BY c.effective_date DESC
        ) AS rank
    FROM {{ ref('transaction_line_base') }} tlb
    LEFT JOIN {{ ref('item_base') }} i ON tlb.item_id = i.item_id
    LEFT JOIN {{ ref('location_base') }} l ON tlb.location_id = l.location_id
    LEFT JOIN {{ ref('bin_base') }} b ON tlb.bin_id = b.bin_id
    LEFT JOIN {{ ref('inventory_status_base') }} s ON tlb.inventory_status_id = s.inventory_status_id
    LEFT JOIN {{ ref('costs_base') }} c
        ON tlb.item_id = c.item_id
        AND tlb.location_id = c.location_id
        AND c.effective_date <= tlb.transaction_date
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
WHERE rank = 1  
GROUP BY date, location, bin, status, item
