# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCD TYPE 1**

# COMMAND ----------

# MAGIC %md
# MAGIC **CREATE FLAG PARAMETER**

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag=dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(*) from parquet.`abfss://silver@carsankidatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch Relative table**

# COMMAND ----------

df_source=spark.sql('''
Select DISTINCT(BRANCH_ID) as Branch_Id,BranchName from parquet.`abfss://silver@carsankidatalake.dfs.core.windows.net/carsales`''')

# COMMAND ----------

# MAGIC %md
# MAGIC **dim_model_sink initial and incremental**

# COMMAND ----------

if  spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink=spark.sql('''Select dim_branch_key,Branch_Id,BranchName from cars_catalog.gold.dim_branch''')
else:
    df_sink=spark.sql('''Select 1 as  dim_branch_key, Branch_Id,BranchName from parquet.`abfss://silver@carsankidatalake.dfs.core.windows.net/carsales` where 1=0 ''')


# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering New Records Vs Old Records**

# COMMAND ----------

df_filter=df_source.join(df_sink,df_source["Branch_Id"]==df_sink["Branch_Id"],'left').select(df_source['Branch_Id'],df_source['BranchName'], df_sink["dim_branch_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter("dim_branch_key IS NOT NULL")

# COMMAND ----------

df_filter_old.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Vs Old Records

# COMMAND ----------

df_filter_new=df_filter.filter(expr("dim_branch_key IS NULL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch the max surrogate key from existing table

# COMMAND ----------

if (incremental_flag=='0'):
    max_value=1
else:
    max_value_df=spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
    max_value=max_value_df.collect()[0][0]+1

print(max_value)      


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key and add max surrogate key

# COMMAND ----------

df_filter_new=df_filter_new.withColumn('dim_branch_key',monotonically_increasing_id()+ max_value)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating final table with df_filter_old and df_filter_new

# COMMAND ----------

df_final=df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD TYPE 1 UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_tbl=DeltaTable.forPath(spark,"abfss://gold@carsankidatalake.dfs.core.windows.net/dim_branch")
    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_branch_key=src.dim_branch_key")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()        

#Initial RUN
else:
    df_final.write.format("delta")\
        .mode('append') \
        .option("path","abfss://gold@carsankidatalake.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.gold.dim_branch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_branch