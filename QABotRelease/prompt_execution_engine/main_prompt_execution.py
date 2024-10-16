# Databricks notebook source
import pandas as pd
import json
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("account_id","")
dbutils.widgets.text("account_name","")
dbutils.widgets.text("env","")
dbutils.widgets.text("component_id","")
dbutils.widgets.text("timeout_seconds","")
dbutils.widgets.text("num_workers","")

account_id=dbutils.widgets.get("account_id")
account_name=dbutils.widgets.get("account_name")
env=dbutils.widgets.get("env")
component_id=dbutils.widgets.get("component_id")
timeout_seconds=int(dbutils.widgets.get("timeout_seconds"))
num_workers=int(dbutils.widgets.get("num_workers"))

# Query the table to get the ParameterJson
query = f"""
SELECT ParameterJson
FROM {env}_genai_configuration.genai_audit_config.parameter
WHERE ComponentId = '{component_id}' AND AccountId = '{account_id}' and IsEnable='True'
"""

# Execute the query and get the result
parameter_json_df = spark.sql(query)
parameter_json_str = parameter_json_df.collect()[0]['ParameterJson']

# Parse the JSON string into a dictionary
parameters = json.loads(parameter_json_str)
print(parameters)

# COMMAND ----------

openai_ke=parameters['openai_ke']
# openai_ke="41cde3efe9cf4764a7fa96c264ec05f2"

api_bas=parameters['api_bas']
# api_bas="https://aedifqabotopenai.openai.azure.com/"

api_versio=parameters['api_versio']
scope=parameters['scope']
promptscatalogName=parameters['promptscatalogName']
promptschemaName=parameters['promptschemaName']
promptstableName=parameters['promptstableName']
account_Id=account_id
promptsfieldName=parameters['promptsfieldName']
#fieldName=parameters['fieldName']
genai_modelName=parameters['genai_modelName']
# genai_modelName="qabot_prompt"
#script_guid=parameters['script_guid']
storage_account_name=parameters['storage_account_name']
catalogName=parameters['catalogName']
schemaName=parameters['schemaName']
tableName=parameters['tableName']
# threshold=parameters['threshold'] #uncomment
threshold = 3000 #parameterize & comment/remove
stage_catalog_name=parameters['stage_catalog_name']
stage_container_name=parameters['stage_container_name']
pii_catalog_name=parameters['pii_catalog_name']

catalogName=env+'_'+catalogName
promptscatalogName=env+'_'+promptscatalogName

stage_catalog_name=env+'_'+stage_catalog_name
stage_container_name=env+'-'+stage_container_name
pii_catalog_name=env+'_'+pii_catalog_name

monthyear = datetime.now().strftime('%m%Y')
tableName=tableName+'_'+monthyear

print(catalogName,tableName)

# COMMAND ----------

pii_df = spark.sql(f"Select *,lower(is_qacompleted) lower_is_qacompleted from {catalogName}.{schemaName}.{tableName} ").toPandas()

# COMMAND ----------

pii_df = pii_df[pii_df['lower_is_qacompleted'] == 'false'].reset_index(drop=True)
pii_df

# COMMAND ----------

import sys
import math
max_workers = num_workers
num_workers = math.ceil(len(pii_df)/2)
if num_workers > max_workers:  
  num_workers = max_workers
elif num_workers < 1:
  print(f"Number of workers: {num_workers}")
  dbutils.notebook.exit('stop')

print(f"Number of workers: {num_workers}")

# COMMAND ----------

scripts_list = [x for x in pii_df['script_guid']]
scripts_list

# COMMAND ----------


def chunker_list(df, chunk_size):
    return list(df[i::chunk_size] for i in range(chunk_size))

pii_data_chunks = list(chunker_list(scripts_list, num_workers))
len(pii_data_chunks)

# COMMAND ----------

pii_data_chunks

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import json
def extract_json_data(transcriptsToProcess):
  value = transcriptsToProcess
  account_name = value[0].split("_")[0]
  print(account_name)
  print(value)

  return dbutils.notebook.run(path = "./prompt_childexecutionengine", timeout_seconds = 32000, arguments = {
    "account_name": account_name,
    "transcriptsToProcess":str(value),
    "catalogName":catalogName,
    'schemaName':schemaName , 
    'tableName': tableName,
    'openai_ke':openai_ke,
    'api_bas':api_bas,
    'api_versio':api_versio,
    'scope':scope,
    'promptscatalogName':promptscatalogName,
    'promptschemaName':promptschemaName,
    'promptstableName':promptstableName,
    'account_Id':account_Id,
    'catalogName':catalogName,
    'schemaName':schemaName,
    'tableName':tableName,
    'storage_account_name':storage_account_name,
    'promptsfieldName':promptsfieldName,
    'genai_modelName':genai_modelName,
    'threshold':threshold,
    'env':env,
    'stage_catalog_name':stage_catalog_name,
    'stage_container_name':stage_container_name,
    'pii_catalog_name':pii_catalog_name
  })

#Calling process_child_nb function and passing pandas dictonary with file paths for mutiple process
with ThreadPoolExecutor(max_workers=num_workers) as executor:
  results = list(executor.map(extract_json_data, pii_data_chunks))

# COMMAND ----------

print(results)
type(results)

if '"1"' in str(results):
  print("Fail")
  error_flag = 1
else:
  print("Success")
  error_flag = 0

# COMMAND ----------

if error_flag == 1:
  raise Exception("Pipeline got Failed for atleast one of the Script_GUIDs")
else:
  print("Pipeline got Succeeded for all the Script_GUIDs")
