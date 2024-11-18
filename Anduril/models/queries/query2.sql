WITH collapsed_transactions AS (
    -- Collapse all transaction lines by summing quantities for each item and location
    SELECT
        t.item_id,
        t.location_id,
        SUM(t.quantity) AS total_quantity,
        MAX(t.transaction_date) AS latest_transaction_date
    FROM {{ ref('transaction_line_base') }} t
    WHERE t.item_id = 209372
      AND t.transaction_date <= '2022-06-05'
    GROUP BY t.item_id, t.location_id
),
filtered_costs AS (
    -- Match each item and location to the most recent cost effective on or before the cutoff date
    SELECT
        c.item_id,
        c.location_id,
        c.cost,
        ROW_NUMBER() OVER (
            PARTITION BY c.item_id, c.location_id
            ORDER BY c.effective_date DESC
        ) AS rnk
    FROM {{ ref('costs_base') }} c
    WHERE c.effective_date <= '2022-06-05'
),
valid_costs AS (
    -- Filter to keep only the most recent cost for each item and location
    SELECT
        item_id,
        location_id,
        cost
    FROM filtered_costs
    WHERE rnk = 1
),
joined_data AS (
    -- Join the collapsed transactions with the most recent costs
    SELECT
        ct.item_id,
        ct.location_id,
        ct.total_quantity,
        vc.cost,
        (ct.total_quantity * vc.cost) AS value
    FROM collapsed_transactions ct
    LEFT JOIN valid_costs vc
      ON ct.item_id = vc.item_id
      AND ct.location_id = vc.location_id
)
-- Calculate the total value of the inventory
SELECT SUM(value) AS total_inventory_value
FROM joined_data
