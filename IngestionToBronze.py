# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Load

# COMMAND ----------

#Dynamic Load for all tables
entities=['customers','drivers','trips','vehicles','payments','locations']

for entity in entities:
    #batch
    df_batch= spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(f"/Volumes/pyspark_project/source/source_data/{entity}/")
    schema_entity= df_batch.schema
    schema_entity

    #incremental
    df= spark.readStream.format("csv")\
        .option("header","true")\
        .schema(schema_entity)\
        .load(f"/Volumes/pyspark_project/source/source_data/{entity}/")

    df.writeStream.format("delta")\
        .option("checkpointLocation", f"/Volumes/pyspark_project/bronze/checkpoint/{entity}/")\
        .outputMode("append")\
        .trigger(once=True)\
        .toTable(f"pyspark_project.bronze.{entity}")