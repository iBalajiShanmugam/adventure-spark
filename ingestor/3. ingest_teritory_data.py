# Databricks notebook source
from pyspark.sql.types import StringType,IntegerType, StructType, StructField

territories_df_schema =  StructType([
    StructField('SalesTerritoryKey', IntegerType()),
    StructField('Region', StringType()),
    StructField('Country', StringType()),
    StructField('Continent', StringType()),
    StructField('manager', StringType())
])

# COMMAND ----------

territories_df = spark.read \
    .format('csv') \
    .options(header=True, inferSchmema=True, sep='\t') \
    .schema(territories_df_schema) \
    .load('dbfs:/FileStore/adventure/AdventureWorks_Territories.csv')

    

# COMMAND ----------

final_territories_df = territories_df \
    .withColumnRenamed('SalesTerritoryKey', 'territory_id') \
    .drop('manager')

# COMMAND ----------

final_territories_df.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('adventure.territories')

# COMMAND ----------


