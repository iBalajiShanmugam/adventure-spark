# Databricks notebook source
import concurrent.futures

# List of child notebook paths
child_notebook_paths = [
    "1. ingest_customer_dat",
    "2. ingest_products_data",
    "3. ingest_teritory_data",
    "4. ingest_sales_data",
    "5. ingest_return_data"
]

# Function to run a child notebook
def run_child_notebook(child_notebook_path):
    result = dbutils.notebook.run(child_notebook_path, 0)


# Run child notebooks in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = executor.map(run_child_notebook, child_notebook_paths)


# COMMAND ----------

import concurrent.futures

# List of child notebook paths
child_notebook_paths = [
    "1. ingest_customer_data",
    "2. ingest_products_data",
    "3. ingest_teritory_data",
    "4. ingest_sales_data",
    "5. ingest_return_data"
]

# Function to run a child notebook
def run_child_notebook(child_notebook_path):
    try:
        result = dbutils.notebook.run(child_notebook_path, 0)
        return {"notebook_path": child_notebook_path, "status": "success"}
    except Exception as e:
        return {"notebook_path": child_notebook_path, "status": "failed", "error": str(e)}

# Run child notebooks in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = executor.map(run_child_notebook, child_notebook_paths)

# Collect the results
results = list(futures)

# Print the execution status of each notebook
for result in results:
    notebook_path = result["notebook_path"]
    status = result["status"]
    if status == "success":
        print(f"Notebook '{notebook_path}' executed successfully.")
    else:
        error = result["error"]
        print(f"Notebook '{notebook_path}' execution failed with error: {error}")


# COMMAND ----------


