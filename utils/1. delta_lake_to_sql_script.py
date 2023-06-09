# Databricks notebook source

# Set the current database
spark.sql("USE adventure")

# Get the list of tables in the database
tables = spark.catalog.listTables()

sql_script = "CREATE DATABASE IF NOT EXISTS adventure;\n"
sql_script += "USE adventure;\n"

# Generate T-SQL scripts for each table and insert data
for table in tables:
    table_name = table.name

    # Get the schema of the table
    schema = spark.table(table_name).schema

    # Generate the CREATE TABLE statement
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    for field in schema.fields:
        column_name = field.name
        column_type = field.dataType.simpleString()
        create_table_sql += f"  {column_name} {column_type},\n"
    create_table_sql = create_table_sql.rstrip(",\n") + "\n);\n"

    # Generate the INSERT INTO statements
    insert_sql = f"INSERT INTO {table_name} VALUES\n"
    data = spark.table(table_name).collect()
    for row in data:
        values = [f"'{str(row[column_name])}'" for column_name in schema.fieldNames()]
        insert_sql += f"  ({', '.join(values)}),\n"
    insert_sql = insert_sql.rstrip(",\n") + ";\n"

    sql_script += create_table_sql
    sql_script += insert_sql
    
   # Write the T-SQL scripts to a file
with open("adventure_script.sql", "w") as file:
    file.write(sql_script)



# COMMAND ----------


