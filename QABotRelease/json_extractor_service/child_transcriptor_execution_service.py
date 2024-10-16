# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import os
import pandas as pd
import json
import datetime as dt
import time
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

dbutils.widgets.text("pathToProcess", "")
dbutils.widgets.text("rootKey", "")
dbutils.widgets.text("account", "")
dbutils.widgets.text("container", "")
dbutils.widgets.text("scope","")


# COMMAND ----------

file_paths = dbutils.widgets.get("pathToProcess")
rootKey = dbutils.widgets.get("rootKey")
account = dbutils.widgets.get("account")
#catalog = dbutils.widgets.get("catalog")
container = dbutils.widgets.get("container")
scope = dbutils.widgets.get("scope")
connection_string_key = dbutils.widgets.get("connection_string_key")
storage_account_name = dbutils.widgets.get("storage_account_name")
blob_acc_url = f"https://{storage_account_name}.blob.core.windows.net"
connection_string = dbutils.secrets.get(scope, key=connection_string_key)

# COMMAND ----------

def get_positions_of_long_strings(input_list):
    get_list = [index for index, string in enumerate(input_list) if len(string) > 30]
    return (get_list[0] if len(get_list) > 0 else ['NA'])

# COMMAND ----------

from azure.storage.blob import BlobClient,ContainerClient
import pandas as pd
import json
import ast

uid_list = []
customer_list = []
error_list = []
agent_list = []
is_unauthenticated = []
isErrorScript = []
file_paths_list = ast.literal_eval(file_paths)
def json_extractor(file_paths_list, rootKey):
  for file_path in file_paths_list:
    print(file_path)
    split_list = file_path.split("/")
    guid_index = get_positions_of_long_strings(split_list)
    print(guid_index)
    guid = split_list[guid_index].replace('-', '')
    uid = str(account) + "_" + str(guid) + "_" + str(file_path.split("/")[-1].split(".")[0])
    print(uid)
    uid_list.append(uid)
    blob = BlobClient(account_url=blob_acc_url,
                    container_name=container,
                    blob_name=file_path,
                    credential=connection_string)

    data = json.load(blob.download_blob())

    if rootKey in data.keys():
      isErrorScript.append(False)
      error_list.append('NA')
      channels_check_list = [x.get('channel') for x in data[rootKey] if 'channel' in x.keys()]
      print("Available transcript channels: ", channels_check_list)
      if 0 in channels_check_list:
        try:
          cust_script = [x.get('display') for x in data[rootKey] if x.get('channel')==0][0]
        except Exception as e:
          print(e)
          cust_script = ''
        if cust_script:
          customer_list.append(cust_script)
          if 'unauthenticated' in cust_script.lower():
            is_unauthenticated.append(True)
          else:
            is_unauthenticated.append(False)
          if "windows, the license got expired." in cust_script.lower():
            agent_script = 'NA'
            agent_list.append(agent_script)
            isErrorScript[-1] = True
            error_list[-1] = "windows, the license got expired."
        else:
          customer_list.append('NA')
          is_unauthenticated.append('NA')
          isErrorScript[-1] = True
          error_list[-1] = "Json is not in expected format"
      else:
        customer_list.append('NA')
        is_unauthenticated.append('NA')
        isErrorScript[-1] = True
        error_list[-1] = "Customer transcript not found"
      
      #Agent Transcript
      if 1 in channels_check_list:
        agent_script = [x.get('display') for x in data[rootKey] if x.get('channel')==1][0]
        if agent_script:
          agent_list.append(agent_script)
        else:
          agent_list.append("NA") 
          isErrorScript[-1] = True
          error_list[-1] = "Json is not in expected format"
      else:
        agent_list.append("NA") 
        isErrorScript[-1] = True
        error_list[-1] = "Agent transcript not found"
            
    elif 'details' in data.keys():
      isErrorScript.append(True)
      customer_list.append("NA")
      agent_list.append("NA")
      is_unauthenticated.append('NA')
      error_list.append(data.get('details'))
    else:
      isErrorScript.append(True)
      customer_list.append("NA")
      agent_list.append("NA")
      is_unauthenticated.append('NA')
      error_list.append('NA')
  return uid, customer_list, agent_list, isErrorScript, is_unauthenticated, error_list

# COMMAND ----------

uid, customer_list, agent_list, isErrorScript, is_unauthenticated, error_list = json_extractor(file_paths_list, rootKey)

# COMMAND ----------

print(len(customer_list))
print(len(agent_list))
print(len(isErrorScript))
print(len(is_unauthenticated))
print(len(error_list))

# COMMAND ----------

print(len(customer_list), customer_list)
print(len(agent_list), agent_list)
print(len(isErrorScript), isErrorScript)
print(len(is_unauthenticated), is_unauthenticated)
print(len(error_list), error_list)

# COMMAND ----------

# DBTITLE 1,Script_json creation

# transcript_list = []
# ex_dict = {}
def list_to_dict(ex_dict, curr_list, transcript_list):
  for i in range(len(curr_list)):
    ex_dict[col_name] = curr_list[i]
    transcript_list.append(ex_dict)
  return transcript_list

# COMMAND ----------

# Dictionarys containing info from each .wav.json file 
dict = {'Script_GUID': uid_list, 'Script_Agent': agent_list, 'Script_Customer': customer_list, 'isErrorScript': isErrorScript, 'isUnauthenticated': is_unauthenticated, 'PII_Scrubbed': 'In Progress', 'Details': error_list}
call_script_df = pd.DataFrame(dict)

# COMMAND ----------

call_script_df

# COMMAND ----------

dbutils.notebook.exit(dict)
