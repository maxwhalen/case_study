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

    * The first step was creating base models for each of the tables, where I did a little pre-processing to account for type errors I found at load time and rename columns where necessary, in order to write more readable queries.

    



