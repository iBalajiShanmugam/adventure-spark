# Databricks notebook source
root_path = 'abfss://adventure@formula1dldataset.dfs.core.windows.net'
customer_file = root_path+"/AdventureWorks_Customers.csv"

# COMMAND ----------

customer_df = spark.read\
    .format('csv') \
    .option("header",True) \
    .option('inferSchema', True) \
    .option('sep', ',') \
    .load(customer_file)

# customer_df = spark.read.csv(customer_file, header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import lit, concat, year
import pyspark.sql.functions as F
customer_df_with_full_name = customer_df \
    .withColumn('FullName', (concat(customer_df.FirstName,lit(' '),customer_df.LastName))) \
    .withColumn('currentAge', year(F.current_date())- year("BirthDate"))
display(customer_df_with_full_name)

# COMMAND ----------




# COMMAND ----------


