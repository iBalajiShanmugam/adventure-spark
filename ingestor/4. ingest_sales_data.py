# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
sales_schema = StructType([
     StructField('OrderDate', StringType()),
     StructField('StockDate', StringType()),
     StructField('OrderNumber', StringType()),
     StructField('ProductKey', IntegerType()),
     StructField('CustomerKey', IntegerType()),
     StructField('TerritoryKey', IntegerType()),
     StructField('OrderLineItem', IntegerType()),
     StructField('OrderQuantity', IntegerType())
])

# COMMAND ----------

sales_df = spark.read \
    .csv('dbfs:/FileStore/adventure/sales/', header=True, schema=sales_schema, sep=',')

# COMMAND ----------

display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, year
final_df = sales_df \
    .withColumnRenamed('OrderDate', 'order_date') \
    .withColumnRenamed('StockDate', 'stock_date') \
    .withColumnRenamed('OrderNumber', 'order_number') \
    .withColumnRenamed('ProductKey', 'product_id') \
    .withColumnRenamed('customerKey', 'customer_id') \
    .withColumnRenamed('TerritoryKey', 'territory_id') \
    .withColumnRenamed('OrderLineItem', 'order_line_item') \
    .withColumnRenamed('OrderQuantity', 'order_quantity') \
    .withColumn('order_date', to_date(col('order_date'), "M/d/yyyy")) \
    .withColumn('stock_date', to_date(col('order_date'), "M/d/yyyy")) \
    .withColumn('year', year(col('order_date')))

# COMMAND ----------

final_df.write \
    .mode('overwrite') \
    .partitionBy('year') \
    .format('delta') \
    .saveAsTable('adventure.sales')

# COMMAND ----------


