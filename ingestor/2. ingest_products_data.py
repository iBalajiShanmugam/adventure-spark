# Databricks notebook source
# MAGIC %fs
# MAGIC ls /FileStore/Adventure

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning and shaping Product table

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField, DoubleType

product_schema = StructType([
        StructField('ProductKey', IntegerType()),
        StructField('ProductSubcategoryKey', IntegerType()),
        StructField('ProductSKU', StringType()),
        StructField('ProductName', StringType()),
        StructField('ModelName', StringType()),
        StructField('ProductDescription', StringType()),
        StructField('ProductColor', StringType()),
        StructField('ProductSize', StringType()),
        StructField('ProductStyle', StringType()),
        StructField('ProductCost', DoubleType()),
        StructField('ProductPrice', DoubleType())
])
 

# COMMAND ----------

product_df =  spark.read.csv(
    'dbfs:/FileStore/Adventure/AdventureWorks_Products.csv',
    sep=',',
    schema=product_schema,
    header=True
)

# COMMAND ----------

from pyspark.sql.functions import round, col
product_column_renamed_df = (product_df
    .withColumnRenamed('ProductKey', 'product_id')
    .withColumnRenamed('ProductSubcategoryKey', 'product_subcategory_id')
    .withColumnRenamed('ProductSKU','product_sku')
    .withColumnRenamed('ProductName','product_name')
    .withColumnRenamed('ModelName','model_name')
    .withColumnRenamed('ProductDescription', 'product_description')
    .withColumnRenamed('ProductColor', 'product_color')
    .withColumnRenamed('ProductSize', 'product_size')
    .withColumnRenamed('ProductStyle', 'product_style')
    .withColumnRenamed('ProductCost', 'product_cost')
    .withColumnRenamed('ProductPrice', 'product_price')
    .withColumn('product_cost', round(col('product_cost'),2))
    .withColumn('product_price', round(col('product_price'),2)))
    

# COMMAND ----------

final_df = product_column_renamed_df.drop('product_style', 'product_size')

# COMMAND ----------

final_df.write.mode('overwrite').format('delta').saveAsTable('Adventure.products')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning and shaping Product_Categories

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
product_categories_schema = StructType([
    StructField('ProductCategoryKey', IntegerType()),
    StructField('CategoryName', StringType())
])

# COMMAND ----------

product_categories = spark.read \
    .format('csv') \
    .option('header', True) \
    .option('sep', ',') \
    .schema(product_categories_schema) \
    .load('dbfs:/FileStore/Adventure/AdventureWorks_Product_Categories.csv')

# COMMAND ----------

column_renamed_product_category = product_categories \
    .withColumnRenamed('ProductCategoryKey', 'product_category_id') \
    .withColumnRenamed('CategoryName', 'category_name')

# COMMAND ----------

column_renamed_product_category.write.mode('overwrite').format('delta').saveAsTable('adventure.product_category')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleaning and Shaping Product Sub Category

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
product_sub_category_schema = StructType([
    StructField('ProductSubcategoryKey', IntegerType()),
    StructField('SubcategoryName', StringType()),
    StructField('ProductCategoryKey', IntegerType())
])

# COMMAND ----------

product_sub_category = spark.read \
    .format('csv') \
    .options(header=True, sep=',', inferSchema=True) \
    .schema(product_sub_category_schema) \
    .load('dbfs:/FileStore/Adventure/AdventureWorks_Product_Subcategories.csv')

# COMMAND ----------

column_renamed_product_sub_category = product_sub_category \
    .withColumnRenamed('ProductSubcategoryKey', 'product_sub_category_id') \
    .withColumnRenamed('SubcategoryName', 'sub_category_name') \
    .withColumnRenamed('ProductCategoryKey', 'product_category_key')

# COMMAND ----------

column_renamed_product_sub_category.write \
    .mode('overwrite') \
    .format('delta') \
    .saveAsTable('adventure.product_sub_category')

# COMMAND ----------


