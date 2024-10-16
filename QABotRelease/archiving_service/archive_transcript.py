# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Import libraries
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContainerClient
from concurrent.futures import ThreadPoolExecutor
import json
import pandas as pd

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



scripts_schema_name = parameters['scripts_schema_name']
config_catalog_name = parameters['config_catalog_name']
config_schema_name = parameters['config_schema_name']
scope = parameters['scope']
connection_string_key = parameters['connection_string_key']
script_container = parameters['script_container']
piiscrubbed_catalog_name=parameters['piiscrubbed_catalog_name']
piiscrubbed_schema_name=parameters['piiscrubbed_schema_name']
scripts_catalog_name=parameters['scripts_catalog_name']


config_catalog_name=env+'_'+config_catalog_name
scripts_catalog_name=env+'_' + scripts_catalog_name
piiscrubbed_catalog_name=env+'_'+piiscrubbed_catalog_name

# COMMAND ----------

# DBTITLE 1,variables
monthyear = datetime.now().strftime('%m%Y')
scripts_table_name = 'scripts_' + monthyear

connection_string = dbutils.secrets.get(scope, key=connection_string_key)

if connection_string is None:
  print("Missing required key vault value: connection_string.")

scriptarchive_container = script_container+'archive'
log_table_name = 'log'
accounts_table_name = 'accounts'
component_table_name = 'component'

# num_workers = 4
# timeout_seconds = 3000

# COMMAND ----------

# DBTITLE 1,user defined functions

account_id_result = spark.sql("""select distinct account_id from {0}.{1}.{2} where lower(accountname)=lower('{3}')""".format(config_catalog_name, config_schema_name, accounts_table_name, account_name)).first()
if account_id_result:
  account_id = account_id_result[0]
else:
  account_id = -1
print('account_id:', account_id)

component_id_result = spark.sql("""select distinct component_id from {0}.{1}.{2} where lower(component_name)='archive'""".format(config_catalog_name, config_schema_name, component_table_name)).first()
if component_id_result:
  component_id = component_id_result[0]
else:
  component_id = -1
print('component_id:', component_id)

#logging
def audit_error_logging(guid, file_name, status, error):
  try:
    spark.sql("""insert into {0}.{1}.{2} (log_id, account_id, component_id, guid, file_name, status, details, created_date, updated_date)
select uuid() as log_id,
'{3}' as  account_id,
{4} as component_id,
'{5}' as guid,
'{6}' as file_name,
'{7}' as status,
case when isnull('{8}') then null else '{8}' end as details,
current_timestamp(),
current_timestamp()""".format(config_catalog_name, config_schema_name, log_table_name, account_id, component_id, guid, file_name, status, error))
    #print('log data inserted successfully')
  except Exception as e:
    print('error while inserting data into log table')
    print(e)

#Run archival notebook concurrently
def execute_archive(notebook_parameter):
  value = json.dumps(notebook_parameter)
  #print(value)
  return dbutils.notebook.run(path = "archive_transcript_concurrent", timeout_seconds = timeout_seconds, arguments = {"notebook_parameter":value})

# COMMAND ----------

# DBTITLE 1,creating blob and container clients
archive_container_client = ContainerClient.from_connection_string(connection_string, scriptarchive_container)

# COMMAND ----------

# DBTITLE 1,creating archive container if it doesn't exists
if archive_container_client.exists():
  print('archive container exists')

else:
  archive_container_client.create_container()
  print('archive container was not exist but created')

# COMMAND ----------

# DBTITLE 1,source to archive blob copy
script_df = spark.sql("""SELECT distinct 
'{0}' as account_name,
'{1}' as scripts_catalog_name,
'{2}' as scripts_schema_name,
'{3}' as scripts_table_name,
'{4}' as scope,
'{5}' as connection_string_key,
'{6}' as script_container,
'{7}' as config_catalog_name,
'{8}' as config_schema_name,
'{9}' as account_id,
'{10}' as component_id,
split_part(Script_GUID,'_',2) as guid
FROM {1}.{2}.{3} where lower(isAudioArchived) = 'false' --and lower(isErrorScript) = 'false'
""".format(account_name, scripts_catalog_name, scripts_schema_name, scripts_table_name, scope, connection_string_key, script_container, config_catalog_name, config_schema_name, account_id, component_id))
#display(script_df)

try:
  if script_df.count() > 0:
    script_df_pan = script_df.select("*").toPandas() #pandas df
    d = script_df_pan.transpose().to_dict()
    dic = list(d.values()) #creating dictonary
    print(dic)

    res =[]
    res_columns = ['guid', 'archival_copy_status', 'details']

  else:
    print('There is no data in script table. Hence exiting the notebook')
    dbutils.notebook.exit(0)

except Exception as e:
  print("Unable to archive the transcripts.")
  print(str(e))
  dbutils.notebook.exit(e)

# COMMAND ----------

# DBTITLE 1,calling concurrent notebooks
#Calling child notebook and passing pandas dictonary for mutiple process
with ThreadPoolExecutor(max_workers=num_workers) as executor:
  #executor.map(execute_presidio, dic)
  results = executor.map(execute_archive, dic)
  for r in results:
    res=res+[json.loads(r)] #unpacking string values

# COMMAND ----------

# DBTITLE 1,getting results from concurrent notebooks
pdf = pd.DataFrame(res)
#display(pdf)
sdf = spark.createDataFrame(pdf) 
display(sdf)

sdf.createOrReplaceTempView('temp_archived')

# COMMAND ----------

# DBTITLE 1,updating isArchived flag in scripts table
# spark.sql("""merge into {0}.{1}.{2} as t
# using temp_archived as s
# on t.Script_GUID like concat('%',s.guid,'%') and lower(s.archival_copy_status) = 'succeeded'
# when matched then update set
# t.isArchived = 'true';
# """.format(scripts_catalog_name, scripts_schema_name, scripts_table_name))
#future refrence for isScriptArchived 

# COMMAND ----------

dbutils.notebook.exit("Success")
