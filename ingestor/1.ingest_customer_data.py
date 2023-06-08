# Databricks notebook source
# MAGIC %sql
# MAGIC Create database if not EXISTS  Adventure
# MAGIC location '/FileStore/processed/'

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructField, StructType

customer_schema = StructType([
 StructField('CustomerKey', IntegerType()),
 StructField('Prefix', StringType()),
 StructField('FirstName', StringType()),
 StructField('LastName', StringType()),
 StructField('BirthDate', StringType()),
 StructField('MaritalStatus', StringType()),
 StructField('Gender', StringType()),
 StructField('EmailAddress', StringType()),
 StructField('AnnualIncome', StringType()),
 StructField('TotalChildren', IntegerType()),
 StructField('EducationLevel', StringType()),
 StructField('Occupation', StringType()),
 StructField('HomeOwner', StringType())
])


# COMMAND ----------

customer_df = spark.read.csv(
   'dbfs:/FileStore/Adventure/AdventureWorks_Customers.csv',
    sep='|',
    schema = customer_schema,
    header=True
)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, to_date, col
from pyspark.sql.types import IntegerType
customer_df = customer_df.withColumn('AnnualIncome', regexp_replace('AnnualIncome','[$,]','').cast(IntegerType())) \
    .withColumn('BirthDate', to_date(col('BirthDate'), 'M/d/yyyy'))

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, initcap
column_rename_customer = (customer_df
     .withColumnRenamed('CustomerKey', 'customer_id')
     .withColumnRenamed('Prefix', 'prefix')
     .withColumnRenamed('FirstName','first_name')
     .withColumnRenamed('LastName','last_name')
     .withColumnRenamed('BirthDate','birth_date')
     .withColumnRenamed('MaritalStatus','marital_status')
     .withColumnRenamed('Gender','gender')
     .withColumnRenamed('EmailAddress','email_address')
     .withColumnRenamed('AnnualIncome','annual_income')
     .withColumnRenamed('TotalChildren','total_children')
     .withColumnRenamed('EducationLevel','education_level')
     .withColumnRenamed('Occupation','occupation')
     .withColumnRenamed('HomeOwner','home_owner')
     .withColumn('full_name', initcap(concat(col('prefix'),lit(' '),col('first_name'),lit(' '),col('last_name')))))

# COMMAND ----------

column_rename_customer.write.mode('overwrite').format('delta').saveAsTable('Adventure.customers')

# COMMAND ----------


