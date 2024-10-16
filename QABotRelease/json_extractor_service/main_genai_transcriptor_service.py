# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContainerClient,BlobClient
import uuid
import os
import pandas as pd
import json
import pyodbc
import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor
import sys
import math

# COMMAND ----------

workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')
print(workspaceUrl)

env='cqa'

# COMMAND ----------

# DBTITLE 1,Test Data: Not Required
# {'account_id': '0dbdb040-682d-40c3-9cba-8a06fefa056a', 'account_name': 'worley', 'env': 'dev', 'component_id': 2, 'timeout_seconds': 3600, 'num_workers': 15}

# COMMAND ----------

dbutils.widgets.text("account_id","")
dbutils.widgets.text("account_name","")
dbutils.widgets.text("component_id","")
dbutils.widgets.text("timeout_seconds","")
dbutils.widgets.text("num_workers","")

account_id=dbutils.widgets.get("account_id")
account_name=dbutils.widgets.get("account_name")
component_id=dbutils.widgets.get("component_id")
timeout_seconds=int(dbutils.widgets.get("timeout_seconds"))
num_workers=int(dbutils.widgets.get("num_workers"))

# Query the table to get the ParameterJson
query = f"""
SELECT ParameterJson
FROM {env}_genai_configuration.genai_audit_config.parameter
WHERE ComponentId = '{component_id}' AND AccountId = '{account_id}' AND IsEnable='True'
"""

# Execute the query and get the result
parameter_json_df = spark.sql(query)
parameter_json_str = parameter_json_df.collect()[0]['ParameterJson']

# Parse the JSON string into a dictionary
parameters = json.loads(parameter_json_str)
print(parameters)

# COMMAND ----------

#Input Parameters
storage_account = parameters["storage_account_name"]
container = parameters["Container"]
script_container = account_name + container
adls_storage_account=parameters["adls_storage_account_name"]
connection_string_key=parameters["blob_service_client_constr"]
blob_client_constr_key = parameters["blob_client_constr_key"]
out_catalog=parameters["out_catalog"]
out_schema=parameters["out_schema"]
scope = parameters["scope"]

connection_string = dbutils.secrets.get(scope, key=connection_string_key)
storage_account_url = 'https://'+storage_account+'.blob.core.windows.net'


adls_container = f"{env}-genai-stg"
script_container = account_name + container
out_catalog=f"{env}_{out_catalog}"



if connection_string is None:
  print("Missing required key vault value: difqabotdwsconstr.")

# COMMAND ----------

blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
source_container_client = ContainerClient.from_connection_string(connection_string, script_container)

# COMMAND ----------

# DBTITLE 1,Get list of file paths
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# connection_string = "my_connection_string"
blob_svc = BlobServiceClient.from_connection_string(conn_str=connection_string)
try:
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")
    print("\nListing blobs...")
    containers = blob_svc.list_containers()
    files_list = []
    container_client = blob_svc.get_container_client(container)
    blob_list = container_client.list_blobs()
    for blob in blob_list:
      if '.json' in blob.name:
        files_list.append(blob.name)      

except Exception as ex:
    print('Exception:')
    print(ex) 

# COMMAND ----------

dict = [{'Script_path': path} for path in files_list]

# COMMAND ----------

print(dict)
res =[] #Appending result into res variable

# COMMAND ----------

max_workers = num_workers
num_workers = math.ceil(len(files_list)/2)
if num_workers > max_workers:  
  num_workers = max_workers
elif num_workers < 1:
  print(f"Number of workers: {num_workers}")
  dbutils.notebook.exit('stop')

print(f"Number of workers: {num_workers}")

# COMMAND ----------

files_list

# COMMAND ----------

#Dynamic chunks
def chunker_list(file_paths, chunk_size):
    return (file_paths[i::chunk_size] for i in range(chunk_size))

#Adaptive chunk_size
chunk_size = round(len(files_list)/num_workers)
print("chunk_size: ", chunk_size)

files_list_chunks = [x for x in list(chunker_list(files_list, chunk_size)) if x]
files_list_chunks

# COMMAND ----------

#Run Json transcriptor NB concurrently
def extract_json_data(pathsListToProcess):
  #value = pathToProcess.get('Script_path')
  return dbutils.notebook.run(path = "./child_transcriptor_execution_service", timeout_seconds = 600, arguments = 
  {"pathToProcess":str(pathsListToProcess), 
    'rootKey': 'combinedRecognizedPhrases', 
    'account': account_name,
    'container': container,
    'scope':scope,
    'storage_account_name':storage_account,
    'connection_string_key':blob_client_constr_key}
  )

#Calling process_child_nb function and passing pandas dictonary with file paths for mutiple process
with ThreadPoolExecutor(max_workers=num_workers) as executor:
  results = list(executor.map(extract_json_data, files_list_chunks))

# COMMAND ----------

import ast
dis_list = [ast.literal_eval(x) for x in results]

# COMMAND ----------

dis_list

# COMMAND ----------

Script_GUID, Script_Customer, Script_Agent, isErrorScript, isUnauthenticated, Details = [], [], [], [], [], []

for x in dis_list:
  Script_GUID = Script_GUID + x.get('Script_GUID')
  Script_Agent = Script_Agent + x.get('Script_Agent')
  Script_Customer = Script_Customer + x.get('Script_Customer')
  isErrorScript = isErrorScript + x.get('isErrorScript')
  isUnauthenticated = isUnauthenticated + x.get('isUnauthenticated')
  Details = Details + x.get('Details')

output_df = pd.DataFrame({'Script_GUID':Script_GUID,'Script_Agent':Script_Agent, 'Script_Customer':Script_Customer, 'isErrorScript':isErrorScript, 'isUnauthenticated':isUnauthenticated, 'PII_Scrubbed': '', 'Details': Details, 'isAudioArchived': False})

output_df

# COMMAND ----------

def dbWrite(dbParameters,tableName,tableContents):
    #tableContents.insert(12,'DateInserted','')
    tableContents['Script_CreationDate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tableContents['Script_CreationDate']= pd.to_datetime(tableContents['Script_CreationDate'],format='%Y-%m-%d %H:%M:%S')
    sparkDF = spark.createDataFrame(tableContents.astype(str))
    #sparkDF = sparkDF.withColumn("Script_CreationDate",to_timestamp("Script_CreationDate").cast("timestamp"))
    sparkDF.write.format("delta").option("mergeSchema", "true").mode("append").option("delta.columnMapping.mode", "name").option('delta.minReaderVersion', '2').option('delta.minWriterVersion', '5').save(adlslocation)
    spark.sql("CREATE TABLE if not exists {}.{} USING DELTA LOCATION '{}' WITH (CREDENTIAL `difscaedifsunitycatalog`) ".format(dbParameters['database'],tableName,adlslocation))
   

# COMMAND ----------

from datetime import datetime, timedelta,date,time
stg_tbl_name = "scripts_" + str(datetime.now().strftime('%m%Y'))
print(stg_tbl_name)

# COMMAND ----------

dbParameters = {}
dbParameters['database'] = f'{out_catalog}.{out_schema}'
dbParameters['adlsLocation'] = f'abfss://{adls_container}@{adls_storage_account}.dfs.core.windows.net/{account_name}/'
adlslocation = dbParameters['adlsLocation']+stg_tbl_name+"/"

# COMMAND ----------

try:
  existing_scripts_df = spark.sql(f"Select * from {out_catalog}.{out_schema}.{stg_tbl_name}").toPandas()
  #verify and excluded all available records
  delta_output = output_df[~output_df['Script_GUID'].isin(existing_scripts_df['Script_GUID'])]
  if len(delta_output) > 0:
    #write results to scripts table
    dbWrite(dbParameters, stg_tbl_name, delta_output)
  else:
    print("No new records to process")
except Exception as e:
  print(e)
  #If table is not available in the database, skip delta check and write to the table
  adlslocation= dbParameters['adlsLocation']+stg_tbl_name+"/"
  dbWrite(dbParameters, stg_tbl_name, output_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
