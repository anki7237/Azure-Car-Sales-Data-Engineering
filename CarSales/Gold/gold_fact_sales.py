# Databricks notebook source
# MAGIC %md
# MAGIC # Create Fact table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Silver data

# COMMAND ----------

df_silver=spark.sql("""SELECT * FROM PARQUET.`abfss://silver@carsankidatalake.dfs.core.windows.net/carsales`""")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading all the DIMs

# COMMAND ----------

df_dealer=spark.sql("SELECT * FROM cars_catalog.gold.dim_dealer")
df_branch=spark.sql("SELECT * FROM cars_catalog.gold.dim_branch")
df_model=spark.sql("SELECT * FROM cars_catalog.gold.dim_model")
df_date=spark.sql("SELECT * FROM cars_catalog.gold.dim_date")

# COMMAND ----------

df_fact=df_silver.join(df_branch,df_silver['Branch_Id']==df_branch['Branch_Id'],how='left')\
                .join(df_dealer,df_silver['Dealer_Id']==df_dealer['Dealer_Id'],how='left')\
                .join(df_model,df_silver['Model_Id']==df_model['Model_Id'],how='left')\
                .join(df_date,df_silver['Date_Id']==df_date['Date_Id'],how='left')\
                .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_model['dim_model_key'],df_dealer['dim_dealer_key'],df_date['dim_date_key'])    


# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatbl=DeltaTable.forName(spark,"cars_catalog.gold.factsales")
    deltatbl.alias("trg").merge(df_fact.alias('src'),'trg.dim_branch_key= src.dim_branch_key and trg.dim_dealer_key= src.dim_dealer_key and trg.dim_model_key=src.dim_model_key and trg.dim_date_key=src.dim_date_key' )\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()        
    

else:
        df_fact.write.format('delta')\
                .mode('Overwrite')\
                .option("path","abfss://gold@carsankidatalake.dfs.core.windows.net/factsales")\
                .saveAsTable("cars_catalog.gold.factsales")   


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.factsales