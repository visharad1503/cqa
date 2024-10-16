# Databricks notebook source
# DBTITLE 1,import libraries
from pyspark.sql import *
from pyspark.sql.functions import *
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd

# COMMAND ----------

# DBTITLE 1,notebook parameters
# dbutils.widgets.text("account_name", "")
# dbutils.widgets.text("config_catalog_name", "")
# dbutils.widgets.text("config_schema_name", "")
# dbutils.widgets.text("storage_account_name", "")
# dbutils.widgets.text("reference_container", "")
# dbutils.widgets.text("verification_type", "")
# dbutils.widgets.text("num_workers", "")

dbutils.widgets.text("account_name", "")
dbutils.widgets.text("config_catalog_name", "")
dbutils.widgets.text("config_schema_name", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("reference_container", "")
dbutils.widgets.text("verification_type", "")
dbutils.widgets.text("num_workers", "")

account_name = dbutils.widgets.get("account_name")
config_catalog_name = dbutils.widgets.get("config_catalog_name")
config_schema_name = dbutils.widgets.get("config_schema_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
reference_container = dbutils.widgets.get("reference_container")
verification_type = dbutils.widgets.get("verification_type")
num_workers = dbutils.widgets.get("num_workers")

# COMMAND ----------

# DBTITLE 1,variables
source_dir = verification_type + '_prompts_source'
archive_dir = verification_type + '_prompts_archive'

# num_workers = 3
timeout_seconds = 3000

# COMMAND ----------

# DBTITLE 1,user defined functions
#Run Json transcriptor NB concurrently
def execute_prompts_insert(notebook_parameter):
  value = json.dumps(notebook_parameter)
  #print(value)
  return dbutils.notebook.run(path = "prompts_table_insert_concurrent", timeout_seconds = timeout_seconds, arguments = {"notebook_parameter":value})

# COMMAND ----------

# DBTITLE 1,reading prompt file from ADLS
main_df = None

for i in dbutils.fs.ls(f"abfss://{reference_container}@{storage_account_name}.dfs.core.windows.net/prompts_source/{account_name}/{source_dir}"):
  file_path = i.path
  if account_name in file_path:
    print(account_name, 'file path: ')
    print(file_path)
    file_df = spark.read.text(file_path, wholetext=True)
    file_df = file_df.withColumn('file_path', lit(file_path)).withColumn('config_catalog_name', lit(config_catalog_name)).withColumn('config_schema_name', lit(config_schema_name)).withColumn('account_name', lit(account_name)).withColumn('storage_account_name', lit(storage_account_name)).withColumn('reference_container', lit(reference_container)).withColumn('verification_type', lit(verification_type))
    #display(source_df)
    file_data = file_df.first()[0]
    print('file data: ')
    print(file_data)
    if main_df is not None:
      main_df = main_df.union(file_df)
    else:
      main_df = file_df
      
display(main_df)

# COMMAND ----------

# DBTITLE 1,getting concurrent notebook parameters
print('Total No. of records: ', main_df.count())

try:
  if main_df.count() > 0:
    main_df_pan = main_df.select("*").toPandas() #pandas df
    d = main_df_pan.transpose().to_dict()
    dic = list(d.values()) #creating dictonary
    print(dic)

    res =[]

  else:
    print('There is no data in reference data container for prompts. Hence exiting the notebook')
    dbutils.notebook.exit(0)

except Exception as e:
  print("Unable to get data from reference data container")
  print(str(e))
  dbutils.notebook.exit(e)

# COMMAND ----------

# DBTITLE 1,triggering concurrent execution
#Calling process_child_nb function and passing pandas dictonary for mutiple process
with ThreadPoolExecutor(max_workers=int(num_workers)) as executor:
  #executor.map(execute_presidio, dic)
  results = executor.map(execute_prompts_insert, dic)
  for r in results:
    res=res+[json.loads(r)] #unpacking string values

# COMMAND ----------

# DBTITLE 1,output from concurrent execution
pdf = pd.DataFrame(res)
display(pdf)
