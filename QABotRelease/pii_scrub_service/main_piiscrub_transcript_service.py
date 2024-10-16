# Databricks notebook source
# DBTITLE 1,import libraries
from pyspark.sql.types import *
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import json

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
WHERE ComponentId = '{component_id}' AND AccountId = '{account_id}' AND IsEnable='True'
"""

# Execute the query and get the result
parameter_json_df = spark.sql(query)
parameter_json_str = parameter_json_df.collect()[0]['ParameterJson']

# Parse the JSON string into a dictionary
parameters = json.loads(parameter_json_str)
print(parameters)

# COMMAND ----------

scripts_catalog_name = parameters["scripts_catalog_name"]
scripts_schema_name = parameters["scripts_schema_name"]
piiscrubbed_catalog_name = parameters["piiscrubbed_catalog_name"]
piiscrubbed_schema_name = parameters["piiscrubbed_schema_name"]
config_catalog_name = parameters["config_catalog_name"]
config_schema_name = parameters["config_schema_name"]
storage_account_name = parameters["storage_account_name"]
piiscrubbed_container = parameters["piiscrubbed_container"]
audit_container = parameters["audit_container"]

# COMMAND ----------

# DBTITLE 1,variables
monthyear = datetime.now().strftime('%m%Y')
scripts_table_name = 'scripts_' + monthyear
piiscrubbed_table_name = 'piiscrubbed_' + monthyear
piiscrubbed_entity_details_table_name = 'piiscrubbed_entity_details_' + monthyear
print(scripts_table_name, piiscrubbed_table_name, piiscrubbed_entity_details_table_name)

config_catalog_name=env+'_'+config_catalog_name
piiscrubbed_catalog_name=env+'_'+piiscrubbed_catalog_name
piiscrubbed_container=env+'-'+piiscrubbed_container
scripts_catalog_name=env+'_'+scripts_catalog_name
print(config_catalog_name, piiscrubbed_catalog_name, piiscrubbed_container,scripts_catalog_name)

# COMMAND ----------

# DBTITLE 1,create table for pii scrubbed transcripts
#create piiscrubbed table if not exists
spark.sql("""
create table if not exists {0}.{1}.{2}(
script_guid string,
script_agent_piiscrubbed string,
script_customer_piiscrubbed string,
is_agent_piiscrubbed string,
is_customer_piiscrubbed string,
is_unauthenticated string,
script_createddate timestamp,
script_piiscrubbeddate timestamp,
is_qacompleted string)
USING DELTA
OPTIONS (
  path 'abfss://{5}@{4}.dfs.core.windows.net/{3}/{2}',
  credential 'difscaedifdunitycatalog'
)
PARTITIONED BY (script_guid)
""".format(piiscrubbed_catalog_name, piiscrubbed_schema_name, piiscrubbed_table_name, account_name, storage_account_name, piiscrubbed_container))

#create piiscrubbed entity details table if not exists
# container_loc='dev'+piiscrubbed_entity_details_table_name
spark.sql("""
create table if not exists {0}.{1}.{2}(
account_name string,
script_guid string,
script_agent_analyzer_results string,
script_agent_piiscrubbed_values string,
script_customer_analyzer_results string,
script_customer_piiscrubbed_values string,
script_piiscrubbeddate timestamp)
USING DELTA
OPTIONS (
  path 'abfss://{4}@{3}.dfs.core.windows.net/config/{2}',
  credential 'difscaedifdunitycatalog'
)
PARTITIONED BY (script_guid)
""".format(config_catalog_name, config_schema_name, piiscrubbed_entity_details_table_name, storage_account_name, audit_container))

# COMMAND ----------

# DBTITLE 1,user defined functions
#Run Json transcriptor NB concurrently
def execute_presidio(notebook_parameter):
  value = json.dumps(notebook_parameter)
  #print(value)
  return dbutils.notebook.run(path = "child_piiscrub_transcript_service", timeout_seconds = timeout_seconds, arguments = {"notebook_parameter":value})

# COMMAND ----------

# DBTITLE 1,input transcripts
script_df = spark.sql("""SELECT *, 
'{3}' as piiscrubbed_catalog_name, '{4}' as piiscrubbed_schema_name, '{5}' as piiscrubbed_table_name, '{6}' as config_catalog_name, '{7}' as config_schema_name, '{8}' as piiscrubbed_entity_details_table_name, '{9}' as account_name, '{10}' as scripts_catalog_name, '{11}' as scripts_schema_name, '{12}' as scripts_table_name FROM {0}.{1}.{2} where lower(isErrorScript)='false' and lower(PII_Scrubbed)<>'succeeded' --and Script_GUID='usbank_64a42006b16e42af9b557526ab87d528_5186'
""".format(scripts_catalog_name, scripts_schema_name, scripts_table_name, piiscrubbed_catalog_name, piiscrubbed_schema_name, piiscrubbed_table_name, config_catalog_name, config_schema_name, piiscrubbed_entity_details_table_name, account_name, scripts_catalog_name, scripts_schema_name, scripts_table_name))
display(script_df)

print('Total No. of records: ', script_df.count())

try:
  if script_df.count() > 0:
    script_df_pan = script_df.select("*").toPandas() #pandas df
    d = script_df_pan.transpose().to_dict()
    dic = list(d.values()) #creating dictonary
    print(dic)

    res =[]
    # res_columns = ['script_guid', 'script_agent_piiscrubbed', 'script_customer_piiscrubbed', 'is_agent_piiscrubbed', 'is_customer_piiscrubbed', 'is_unauthenticated', 'script_createddate', 'script_piiscrubbeddate', 'is_qacompleted', 'script_agent_analyzer_results', 'script_agent_piiscrubbed_values', 'script_customer_analyzer_results', 'script_customer_piiscrubbed_values']

  else:
    print('There is no data in script table. Hence exiting the notebook')
    dbutils.notebook.exit(0)

except Exception as e:
  print("Unable to get data from scripts table")
  print(str(e))
  dbutils.notebook.exit(e)

# COMMAND ----------

# DBTITLE 1,concurrent pii scrubbing executions
#Calling process_child_nb function and passing pandas dictonary for mutiple process
with ThreadPoolExecutor(max_workers=num_workers) as executor:
  #executor.map(execute_presidio, dic)
  results = executor.map(execute_presidio, dic)
  for r in results:
    res=res+[json.loads(r)] #unpacking string values

# COMMAND ----------

# DBTITLE 1,output from concurrent execution
pdf = pd.DataFrame(res)
display(pdf)
#sdf = spark.createDataFrame(pdf) 
#display(sdf)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


