WITH relevant_transactions AS (
    SELECT
        t.transaction_line_id,
        t.location_id,
        t.bin_id,
        t.inventory_status_id,
        t.item_id,
        t.quantity,
        t.transaction_date
    FROM {{ ref('transaction_line_base') }} t
    WHERE t.item_id = 355576
      AND t.transaction_date <= '2022-11-21'
),
aggregated_quantities AS (
    SELECT
        rt.location_id,
        rt.bin_id,
        rt.inventory_status_id,
        SUM(rt.quantity) AS total_quantity
    FROM relevant_transactions rt
    GROUP BY rt.location_id, rt.bin_id, rt.inventory_status_id
)
SELECT
    l.location AS location,
    b.bin AS bin,
    s.status AS status,
    aq.total_quantity
FROM aggregated_quantities aq
LEFT JOIN {{ ref('location_base') }} l ON aq.location_id = l.location_id
LEFT JOIN {{ ref('bin_base') }} b ON aq.bin_id = b.bin_id
LEFT JOIN {{ ref('inventory_status_base') }} s ON aq.inventory_status_id = s.inventory_status_id
