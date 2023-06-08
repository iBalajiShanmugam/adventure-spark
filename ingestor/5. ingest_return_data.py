# Databricks notebook source
# MAGIC %md
# MAGIC #### Cleaning and Trasform the Return Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
returns_schema = StructType([
    StructField('ProductKey', IntegerType()),
    StructField('ReturnDate', StringType()),
    StructField('ReturnQuantity', IntegerType()),
    StructField('TerritoryKey', IntegerType())
])

# COMMAND ----------

returns_df = spark.read \
    .format('json') \
    .option('multiLine', True) \
    .schema(returns_schema) \
    .load('dbfs:/FileStore/Adventure/Adventure_returns.json')

# COMMAND ----------

from pyspark.sql.functions import to_date, col, year
final_df = returns_df \
    .withColumn('ReturnDate', to_date(col('ReturnDate'), 'M/d/yyyy')) \
    .withColumn('year', year(col('ReturnDate'))) \
    .withColumnRenamed('ProductKey', 'product_id') \
    .withColumnRenamed('ReturnDate', 'return_date') \
    .withColumnRenamed('ReturnQuantity', 'return_quantity') \
    .withColumnRenamed('TerritoryKey', 'territory_id')

# COMMAND ----------

final_df.write \
    .format('delta') \
    .partitionBy('year') \
    .saveAsTable('adventure.returns')

# COMMAND ----------


