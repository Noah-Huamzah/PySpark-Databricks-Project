# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

# COMMAND ----------

import os
import sys

curDir= os.getcwd()
sys.path.append(curDir)
curDir

# COMMAND ----------

from utils import transformations

# COMMAND ----------

df_cust=spark.read.table("pyspark_project.bronze.customers")
df_cust=df_cust.withColumn("domain",split(col('email'),'@')[1])\
    .withColumn("phone_number",regexp_replace(col('phone_number'),r"[^0-9]",""))\
    .withColumn("full_name",concat_ws(" ",col('first_name'),col('last_name')))\
    .drop('first_name','last_name')


# COMMAND ----------

df_cust.count()

# COMMAND ----------

obj=transformations()
sdf_cust=obj.removeDup(df_cust,['customer_id'],'last_updated_timestamp')
print(df_cust.count())
print(sdf_cust.count())

# COMMAND ----------

sdf_cust= obj.processTimestamp(sdf_cust)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.customers"):
    obj.upsert(
        sdf_cust,
        ['customer_id'],
        'customers',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_cust.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.customers")
    


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pyspark_project.silver.customers

# COMMAND ----------

df_drivers=spark.read.table("pyspark_project.bronze.drivers")
df_drivers=df_drivers\
    .withColumn("phone_number",regexp_replace(col('phone_number'),r"[^0-9]",""))\
    .withColumn("full_name",concat_ws(" ",col('first_name'),col('last_name')))\
    .drop('first_name','last_name')


# COMMAND ----------

objD=transformations()
sdf_drivers=objD.removeDup(df_drivers,['driver_id'],'last_updated_timestamp')
sdf_drivers= objD.processTimestamp(sdf_drivers)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.drivers"):
    objD.upsert(
        sdf_drivers,
        ['driver_id'],
        'drivers',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_drivers.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.drivers")

# COMMAND ----------

df_locations=spark.read.table("pyspark_project.bronze.locations")
objL=transformations()
sdf_locations=objL.removeDup(df_locations,['location_id'],'last_updated_timestamp')
sdf_locations= objL.processTimestamp(sdf_locations)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.locations"):
    objD.upsert(
        sdf_locations,
        ['location_id'],
        'locations',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_locations.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.locations")

# COMMAND ----------

from pyspark.sql.functions import when, col
df_payments=spark.read.table("pyspark_project.bronze.payments")

df_payments = df_payments.withColumn(
    'PaymentMode',
    when(
        ((col('payment_method') == 'Wallet') & (col("payment_status") == 'Success')),
        'Online-Success'
    ).when(
        ((col('payment_method') == 'Wallet') & (col("payment_status") == 'Pending')),
        'Online-Pending'
    ).when(
        ((col('payment_method') == 'Wallet') & (col("payment_status") == 'Failed')),
        'Online-Failure'
    ).when(
        ((col('payment_method') == 'CARD') & (col("payment_status") == 'Success')),
        'Online-Success'
    ).when(
        ((col('payment_method') == 'CARD') & (col("payment_status") == 'Pending')),
        'Online-Pending'
    ).when(
        ((col('payment_method') == 'CARD') & (col("payment_status") == 'Failed')),
        'Online-Failure'
    ).otherwise('Offline')
)

# COMMAND ----------

objP=transformations()
sdf_payments=objP.removeDup(df_payments,['payment_id'],'last_updated_timestamp')
sdf_payments= objP.processTimestamp(sdf_payments)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.payments"):
    objD.upsert(
        sdf_payments,
        ['payment_id'],
        'payments',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_payments.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.payments")

# COMMAND ----------

df_vehicles= spark.read.table("pyspark_project.bronze.vehicles")
df_vehicles= df_vehicles.withColumn("make",upper(col("make")))
objV=transformations()
sdf_vehicles=objV.removeDup(df_vehicles,['vehicle_id'],'last_updated_timestamp')
sdf_vehicles= objV.processTimestamp(sdf_vehicles)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.vehicles"):
    objD.upsert(
        sdf_vehicles,
        ['vehicle_id'],
        'vehicles',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_vehicles.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.vehicles")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pyspark_project.silver.payments limit 10

# COMMAND ----------

df_trips= spark.read.table("pyspark_project.bronze.trips")
objT=transformations()
sdf_trips=objT.removeDup(df_trips,['trip_id'],'last_updated_timestamp')
sdf_trips= objT.processTimestamp(sdf_trips)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("pyspark_project.silver.trips"):
    objD.upsert(
        sdf_trips,
        ['trip_id'],
        'trips',
        'last_updated_timestamp',
        spark
    )
else:
    sdf_trips.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("pyspark_project.silver.trips")