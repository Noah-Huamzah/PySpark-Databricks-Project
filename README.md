Data Engineering project following the Medallion Architecture. 
Excel/CSV source files ingested to Bronze layer on Databricks catalog.
Data from Bronze is transformed using automated helper functions described on utils.py. Upsert logic with timestamp.
DBT to ingest Silver layer from Databricks to add Slowly Changing Dimensions logic and revert the data to Gold layer on Databricks.
Gold layer used to draw meaningful insights on the processed and cleaned data.
