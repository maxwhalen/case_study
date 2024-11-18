WITH latest_date_cte AS (
    SELECT
        MAX(t.transaction_date) AS latest_date
    FROM transaction_line t
),
transaction_data AS (
    SELECT
        t.item_id,
        t.location_id,
        t.bin_id,
        MAX(t.transaction_date) AS last_transaction_date
    FROM transaction_line t
    GROUP BY t.item_id, t.location_id, t.bin_id
),
aging_data AS (
    SELECT
        td.item_id,
        td.location_id,
        td.bin_id,
        ld.latest_date,
        DATE_PART('day', ld.latest_date - td.last_transaction_date) AS inventory_age
    FROM transaction_data td
    CROSS JOIN latest_date_cte ld
)
SELECT
    l.name AS location_name,
    b.name AS bin_name,
    i.name AS item_name,
    AVG(aging_data.inventory_age) AS average_inventory_age
FROM aging_data
LEFT JOIN location l ON aging_data.location_id = l.id
LEFT JOIN bin b ON aging_data.bin_id = b.id
LEFT JOIN item i ON aging_data.item_id = i.id
GROUP BY l.name, b.name, i.name
