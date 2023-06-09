# Databricks notebook source
sales_df = spark.read.format('delta').load('dbfs:/FileStore/processed/sales')

# COMMAND ----------

product_df = spark.read.format('delta').load('dbfs:/FileStore/processed/products')

# COMMAND ----------

product_sub_category_df = spark.read.format('delta').load('dbfs:/FileStore/processed/product_sub_category')

# COMMAND ----------

product_category_df = spark.read.format('delta').load('dbfs:/FileStore/processed/product_category')

# COMMAND ----------

from pyspark.sql.functions import col

product_sales_df = sales_df.alias('s') \
    .join(product_df.alias('p'), col('p.product_id') == col('s.product_id')) \
    .join(product_sub_category_df.alias('ps'), col('ps.product_sub_category_id') == col('p.product_subcategory_id')) \
    .join(product_category_df.alias('pc'), col('pc.product_category_id') == col('ps.product_category_key'))

# COMMAND ----------

#Top 5 product based on orders Quanity
from pyspark.sql.functions import col, count, sum, round

result_df = product_sales_df \
    .groupBy('s.product_id','p.product_name') \
    .agg(count('s.order_quantity').alias('Total_quantity'), round(sum(col('s.order_quantity')*col('p.product_price')),2).alias('Total_revenue')) \
    .orderBy(col('Total_quantity').desc()) \
    .limit(10)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, round

result_df = product_sales_df \
    .groupBy('s.product_id','p.product_name') \
    .agg(count('s.order_quantity').alias('Total_quantity'), round(sum(col('s.order_quantity')*col('p.product_price')),2).alias('Total_revenue')) \
    .orderBy(col('Total_revenue').desc()) \
    .limit(5)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, round

result_df = product_sales_df \
    .groupBy('pc.product_category_id','pc.category_name') \
    .agg(count('s.order_quantity').alias('Total_quantity'), round(sum(col('s.order_quantity')*col('p.product_price')),2).alias('Total_revenue')) \
    .orderBy(col('Total_revenue').desc()) \
    .limit(10)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, round

result_df = product_sales_df \
    .groupBy('pc.product_category_id','pc.category_name','s.year') \
    .agg(count('s.order_quantity').alias('Total_quantity'), round(sum(col('s.order_quantity')*col('p.product_price')),2).alias('Total_revenue')) \
    .orderBy(col('Total_revenue').desc()) \
    .limit(10)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, round

result_df = product_sales_df \
    .groupBy('s.product_id','p.product_color') \
    .agg(count('s.order_quantity').alias('Total_quantity')) \
    .orderBy(col('Total_quantity').desc()) \
    .limit(10)

display(result_df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(distinct product_id) from adventure.products 

# COMMAND ----------


