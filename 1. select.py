# Databricks notebook source
customer_df = (spark.read.format('csv')
                    .option('sep', '|')
                    .option('inferschema', True)
                    .option('header', True)
                    .load('dbfs:/FileStore/Adventure/AdventureWorks_Customers.csv'))

# COMMAND ----------

selected_column = customer_df.select('FirstName', 'LastName')

# COMMAND ----------

selected_column = customer_df.select(customer_df['FirstName'], customer_df['LastName'])

# COMMAND ----------

from pyspark.sql.functions import col
selected_column = customer_df.select(col('FirstName').alias('first_name'), col('LastName').alias('last_name'))

# COMMAND ----------

display(selected_column)

# COMMAND ----------

display(customer_df)

# COMMAND ----------


