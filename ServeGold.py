# Databricks notebook source
# MAGIC %md
# MAGIC ## View for Gold FACT Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view df_view as
# MAGIC select * from pyspark_project.gold.facttrips

# COMMAND ----------

# MAGIC %md
# MAGIC ## Annual Customer Expenditure

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.customer_id,c.full_name, round(sum(v.fare_amount),2) total_fare,year(v.trip_start_time) as year from pyspark_project.gold.customers as c inner join
# MAGIC df_view as v on v.customer_id=c.customer_id
# MAGIC group by c.customer_id,c.full_name,year
# MAGIC sort by total_fare desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Count of Succesfull Trips

# COMMAND ----------

# MAGIC %sql
# MAGIC select trip_status, count(*) from df_view group by trip_status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driver Ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct driver_id,full_name,driver_rating from pyspark_project.gold.drivers order by driver_rating desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## City wise total Trips and Revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.city,l.country,count(*) as total_trips,round(sum(v.fare_amount)) as revenue from df_view v
# MAGIC left join pyspark_project.gold.customers as c on v.customer_id=c.customer_id
# MAGIC left join pyspark_project.gold.locations as l on c.city=l.city
# MAGIC group by c.city,l.country order by total_trips desc