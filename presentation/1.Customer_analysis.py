# Databricks notebook source
customer_df = spark.read \
    .format('delta') \
    .load('dbfs:/FileStore/processed/customers')

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
    .groupBy('occupation') \
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

#Top 10 customer based on their orders

%sql
select 
    *
from 
    customer c
left join 
    sales s on s.customer_id = c.customer_id
grou



# COMMAND ----------


