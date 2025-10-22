# Databricks notebook source
# MAGIC %md
View for Gold FACT Table

# COMMAND ----------

# MAGIC %sql
create or replace view df_view as
select * from pyspark_project.gold.facttrips

# COMMAND ----------

# MAGIC %md
Annual Customer Expenditure

# COMMAND ----------

# MAGIC %sql
select c.customer_id,c.full_name, round(sum(v.fare_amount),2) total_fare,year(v.trip_start_time) as year from pyspark_project.gold.customers as c inner join
df_view as v on v.customer_id=c.customer_id
group by c.customer_id,c.full_name,year
sort by total_fare desc

# COMMAND ----------

# MAGIC %md
Count of Succesfull Trips

# COMMAND ----------

# MAGIC %sql
select trip_status, count(*) from df_view group by trip_status

# COMMAND ----------

# MAGIC %md
Driver Ratings

# COMMAND ----------

# MAGIC %sql
select distinct driver_id,full_name,driver_rating from pyspark_project.gold.drivers order by driver_rating desc

# COMMAND ----------

# MAGIC %md
City wise total Trips and Revenue

# MAGIC %sql
select c.city,l.country,count(*) as total_trips,round(sum(v.fare_amount)) as revenue from df_view v
left join pyspark_project.gold.customers as c on v.customer_id=c.customer_id
left join pyspark_project.gold.locations as l on c.city=l.city
group by c.city,l.country order by total_trips desc
