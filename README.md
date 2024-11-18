# Anduril Take Home Assignment

# Assignment 

## 1. Populating the Database

* Using dbt's seed functionality, we can populate the database with the data in the `seeds` folder.

* After unzipping the CSVs and placing them in the `seeds` folder, we can run the following command to populate the database:

    ```
    dbt seed
    ```

* I chose to specify the column types in dbt_project.yml to ensure that the data types are correct and to catch any obvious data type errors early

At this point our database looks like this:

![Database Schema](images/db_schema.png)

## 2. Creating 'Inventory Daily' Table

* The first step was creating base models for each of the tables, where I did a little pre-processing to account for type errors I found at load time and I renamed columns where necessary in order to write more readable queries.

* I updated my project yml file to include these base models, as well as directories for processing and final models

* I created a model that creates the inventory_daily table using the following SQL query:

    ```
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
    ```





