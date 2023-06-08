# Databricks notebook source
customer_df = (spark.read.format('csv')
                    .option('sep', '|')
                    .option('inferschema', True)
                    .option('header', True)
                    .load('dbfs:/FileStore/Adventure/AdventureWorks_Customers.csv'))

# COMMAND ----------


from pyspark.sql.functions import col, regexp_replace
help(regexp_replace)

# COMMAND ----------

#Show all customers with an annual income greater than $50,000.

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType

#convert AnnualIncome column string to interger
customer_df = customer_df \
    .withColumn('AnnualIncome', regexp_replace('AnnualIncome','[$,]','').cast(IntegerType()))

result_df = customer_df.filter(col('AnnualIncome') > 50000)

display(result_df)

# COMMAND ----------

#Display customers who have a "Mr." prefix in their names

result_df = customer_df.filter(customer_df.Prefix == 'MR.')
display(result_df)

# COMMAND ----------

#Find customers whose first name starts with the letter "A".
result_df = customer_df.filter(customer_df.FirstName.startswith('A'))
display(result_df)

# COMMAND ----------

#List customers who have a birth date before the year 1980.
from pyspark.sql.functions import year
result_df = customer_df.filter(year(col('BirthDate')) < 1980)

display(result_df)

# COMMAND ----------

#Show customers who are married (MaritalStatus equals 'Married').
result_df = customer_df.filter(col('MaritalStatus') == 'M')
display(result_df)

# COMMAND ----------

#Display customers who are female (Gender equals 'Female').
final_df = customer_df.where(col('Gender') == 'F')
display(final_df)

# COMMAND ----------

#Find customers with an email address ending in ".com".
final_df = customer_df.filter(col('EmailAddress').endswith('.com'))
display(final_df)

# COMMAND ----------

#Show customers with at least 2 total children.

final_df = customer_df.filter(col('TotalChildren')>=2)
display(final_df)

# COMMAND ----------

display(customer_df.select('Occupation').distinct())

# COMMAND ----------

#Show customers who have an annual income greater than $80,000, are married, and have a graduate degree.

result_df = customer_df.filter(
    (col('AnnualIncome') > 80000) &
    (col('MaritalStatus') == 'M') &
    (col('EducationLevel') == 'Graduate Degree')
)

display(result_df)

# COMMAND ----------

#Find customers who are homeowners, have at least 2 total children, and work as professionals or managers.
result_df = customer_df.filter(
    (customer_df.HomeOwner == 'Y') &
    (customer_df.TotalChildren >=2) &
    (customer_df.Occupation.isin('Professional','Management') )
)

display(result_df)

# COMMAND ----------

#Display female customers with an annual income between $40,000 and $60,000, and who have either a high school or #college degree.

result_df = customer_df.filter(
    customer_df.AnnualIncome.between(40000,60000) &
    ((customer_df.EducationLevel == 'High School') | (customer_df.EducationLevel == 'Graduate Degree'))
)

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import year
help(year)

# COMMAND ----------


