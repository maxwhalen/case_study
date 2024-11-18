WITH collapsed_transactions AS (
    SELECT
        t.transaction_line_id,
        t.item_id,
        t.location_id,
        SUM(t.quantity) AS net_quantity,
        MAX(t.transaction_date) AS latest_transaction_date
    FROM {{ ref('transaction_line_base') }} t
    WHERE t.transaction_date <= '2022-01-01'
    GROUP BY
        t.transaction_line_id, t.item_id, t.location_id
),
latest_costs AS (
    SELECT
        c.item_id,
        c.location_id,
        c.cost,
        ROW_NUMBER() OVER (
            PARTITION BY c.item_id, c.location_id
            ORDER BY c.effective_date DESC
        ) AS rnk
    FROM {{ ref('costs_base') }} c
    WHERE c.effective_date <= '2022-01-01'
),
filtered_costs AS (
    SELECT
        item_id,
        location_id,
        cost
    FROM latest_costs
    WHERE rnk = 1
),
joined_transactions AS (
    SELECT
        ct.transaction_line_id,
        l.location AS location_name,
        ct.item_id,
        ct.net_quantity,
        ct.latest_transaction_date,
        fc.cost,
        (ct.net_quantity * fc.cost) AS total_value
    FROM collapsed_transactions ct
    LEFT JOIN filtered_costs fc
      ON ct.item_id = fc.item_id
      AND ct.location_id = fc.location_id
    LEFT JOIN {{ ref('location_base') }} l
      ON ct.location_id = l.location_id
),
location_filtered AS (
    SELECT
        transaction_line_id,
        item_id,
        net_quantity,
        cost,
        total_value
    FROM joined_transactions
    WHERE location_name = 'c7a95e433e878be525d03a08d6ab666b'
      AND net_quantity > 0 -- Exclude neg inventory
)
SELECT
    SUM(total_value) AS total_inventory_value
FROM location_filtered

