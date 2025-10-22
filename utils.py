from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

class transformations:
    def removeDup(self, df:DataFrame, cols:List, cdc):
        df = df.withColumn('keys', concat(*[col(c) for c in cols]))
        df = df.withColumn('count', row_number().over(Window.partitionBy('keys').orderBy(desc(cdc))))
        df = df.filter(col('count') == 1).drop('keys', 'count')
        return df
    
    def processTimestamp(self,df):
        df=df.withColumn('processTimestamp',current_timestamp())
        return df
    
    def upsert(self, df, keys, entity, cdc, spark):
        mergeCondition = [f"src.{i} = tgt.{i}" for i in keys]
        deltaObj = DeltaTable.forName(
            spark,
            f"pyspark_project.silver.{entity}"
        )
        deltaObj.alias("tgt").merge(
            df.alias("src"),
            " and ".join(mergeCondition)
        ).whenMatchedUpdateAll(
            condition=f"src.{cdc} >= tgt.{cdc}"
        ).whenNotMatchedInsertAll().execute()
