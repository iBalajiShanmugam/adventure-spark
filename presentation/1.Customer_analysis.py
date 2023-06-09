# Databricks notebook source
customer_df = spark.read \
    .format('delta') \
    .load('dbfs:/FileStore/processed/customers')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select count(customer_id) from adventure.customers

# COMMAND ----------

#Total customer based on Gender
from pyspark.sql.functions import col, count, when
result_df = customer_df \
    .groupBy(col('gender')) \
    .agg(count('customer_id').alias('total')) \
    .withColumn('gender', when(col('gender') == 'M','Male').when(col('gender')=='F','Female').otherwise('Others'))

display(result_df)

# COMMAND ----------

#Total customer based on ocuppation 

result_df = customer_df \
    .groupBy('occupation', 'gender') \
    .agg(count('customer_id').alias('total'))

display(result_df)

# COMMAND ----------

#Total Customer Based on education_level

result_df = customer_df \
    .filter(col('gender').isin(['M','F'])) \
    .groupby('education_level', 'gender') \
    .agg(count('customer_id')) \
    .withColumn('gender', when(col('gender') == 'M','Male').when(col('gender')=='F','Female').otherwise('Others')) 

display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Top 10 customer based on their orders
# MAGIC
# MAGIC select c.full_name, sum(s.order_quantity) as total_quantity
# MAGIC from 
# MAGIC     adventure.customers c
# MAGIC left join 
# MAGIC     adventure.sales s on s.customer_id = c.customer_id
# MAGIC where c.full_name is not null
# MAGIC group by c.full_name
# MAGIC order by total_quantity desc
# MAGIC limit 10
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select year(s.order_date) as year, count(distinct s.customer_id) as customer_count
# MAGIC from adventure.sales s
# MAGIC group by year(s.order_date)

# COMMAND ----------


