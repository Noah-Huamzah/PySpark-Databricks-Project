Data Engineering project following the Medallion Architecture.
Excel/CSV source data was ingested to Bronze layer on Databricks catalogs also using Incremental load.
Ingested data transformed using automated Python functions decribed in utils.py and PySpark notebooks. Upsert logic with timestamp.
DBT to parse Silver layer and add Slowly Changing Dimensions and Star Schema then loading the data back to Databricks Gold layer.
Gold layer used to draw meaningful insights and dashboards from the proccessed and cleaned Data.

Databricks X DBT
