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
            tlb.transaction_date AS date,
            l.location,
            b.bin,
            s.status,
            i.item,
            tlb.quantity,
            c.cost,
            (tlb.quantity * c.cost) AS value,
            ROW_NUMBER() OVER (
                PARTITION BY tlb.item_id, tlb.location_id, tlb.transaction_date
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
    WHERE rank = 1  -- Keep only the closest effective_date
    GROUP BY date, location, bin, status, item;
    ```

* After running `dbt run`, the inventory_daily table was created and looks as follows:

![Inventory Daily Table](images/inventory_daily.png)

After this table is built though, it is clear that is at least one data issue. Starting with the fact that 67 rows do not have a a valid cost attached to them. This occurs because there is not a row in the cost table for that location and item that was creating prior to the transaction date. Some of these issues would be resolved if we use the closest date after was used retroactively, but still there are over 40 combinations of location, bin, transaction date, and inventory status that have no costs associated with them.

The second point to note is the number of rows that have a quantity less than zero. The following query show that over half of the rows (53%) in the transaction_line table have a negative quantity.

    ```
    SELECT
        (COUNT(*) FILTER (WHERE tlb.quantity < 0) * 100.0) / COUNT(*) AS pct_neg
    FROM
        transaction_line_base tlb
    ```

While this may represent a valid business process, maybe it represents items on back order and the financial accounting does represent a negative value, it is worth confirming that the data being collected is accurate.

## 3. Data Quality Check

1. The first data quality concern I had was caught using the seed table creation. I initialized the quantity column in the *transaction_line* table as an integer, assuming orders came in discrete quantities. After loading the data I found 26 instances where quantity was not an integer. This could have been due to a variety of reasons, such as a mis-entered value or a partial order, so I made the quantity column a float to account for the reasonable possibility of partial orders. 

It is equally plausible that this is a data error though, so in the transaction_line_base model I added a column `is_non_integer` to flag these rows for further investigation and so stakeholders are able to make the decision to include or exclude these rows from analysis.

2. A minor detail I noticed was in the seed CSVs the date column for the cost column is "date" whereas in the documentation it is "effective_date". I updated the costs table in the base_model to reflect this.

3. As mentioned above, we have 44 groupings of location, bin, transaction_date, and inventory status that do not have a cost associated with them. This materielized view can be used to identify these rows and investigate the cause of the missing costs.

    ```
    CREATE MATERIALIZED VIEW missing_costs AS
    SELECT DISTINCT  
        tlb.location_id, 
        tlb.bin_id, 
        tlb.transaction_date, 
        tlb.inventory_status_id
    FROM 
        transaction_line_base tlb
    LEFT JOIN 
        costs_base c
        ON tlb.item_id = c.item_id
        AND tlb.location_id = c.location_id
        AND c.effective_date <= tlb.transaction_date
    WHERE 
        c.cost IS NULL;
    ```

4. Checking for duplicates:

    * When grouping the transaction_line table by transaction_id and transaction_line_id we have a number of duplicate rows. We have multiple cases where there are 337 rows with this grouping. Assuming each transactin_line represents a line item on an invoice or similar document it seems illogical that there would be multiple rows with the same transaction_id and transaction_line_id. This issue persists when adding more variables to the grouping, such as location_id, item_id, and bin_id. This could be a data entry error or a system error, but it is worth investigating further.

    * None of the dimension tables appear to have any duplicates.

5. Checking for nulls:

    There are no nulls in either the fact table or dimension tables.

6. Checking for data type errors:

    Using dbt's seed functionality we were able to constrain data types to the desired forms and catch any data type errors early.




