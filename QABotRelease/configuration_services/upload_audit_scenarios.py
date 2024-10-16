# Databricks notebook source
# MAGIC %md
# MAGIC ## Update Audit Scenarios

# COMMAND ----------

# MAGIC %pip install azure-storage-blob
# MAGIC %pip install openpyxl

# COMMAND ----------

import json
import pandas as pd
from azure.storage.blob import BlobClient
from io import BytesIO
import openpyxl

# COMMAND ----------

workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')
print(workspaceUrl)
env='cqa'

# COMMAND ----------

dbutils.widgets.text("account_name","")
account_name=dbutils.widgets.get("account_name")

dbutils.widgets.text("file_name","")
file_path = dbutils.widgets.get("file_name")

component_name='Upload Audit Scenarios'

query = f"""
SELECT account_id
FROM {env}_genai_configuration.genai_audit_config.accounts
WHERE lower(accountName) = lower('{account_name}')
"""

result = spark.sql(query).collect()
account_id=result[0]['account_id']

StandardCallAuditIdQuery = f"""
SELECT account_id
FROM {env}_genai_configuration.genai_audit_config.accounts
WHERE lower(accountName) = lower('StandardCallAudit')
"""

std_acc_id_result = spark.sql(StandardCallAuditIdQuery).collect()
std_acc_id=std_acc_id_result[0]['account_id']
print(std_acc_id)
query = f"""
SELECT 
    c.component_id,
    p.timeout_seconds,
    p.parameter_type,
    p.num_workers
FROM 
    {env}_genai_configuration.genai_audit_config.component c
JOIN 
    {env}_genai_configuration.genai_audit_config.parameter p
ON 
    c.component_id = p.ComponentId
WHERE 
    c.component_name = '{component_name}' and p.AccountId = '{account_id}'
"""
df = spark.sql(query)
print(df)
result = df.collect()[0]
ComponentId = result['component_id']

# Query the table to get the ParameterJson
query = f"""
SELECT ParameterJson
FROM {env}_genai_configuration.genai_audit_config.parameter
WHERE ComponentId = '{ComponentId}' AND AccountId = '{account_id}' and IsEnable='True'
"""

# Execute the query and get the result
parameter_json_df = spark.sql(query)
parameter_json_str = parameter_json_df.collect()[0]['ParameterJson']

# Parse the JSON string into a dictionary
parameters = json.loads(parameter_json_str)
print(parameters)

# COMMAND ----------

file_path = parameters['FilePath']
print(file_path)
connection_string_key=parameters['connection_string_key']
container=parameters['container']
out_container=parameters['out_container']
out_storage_account=parameters['out_storage_account']
storage_account=parameters['storage_account']
scope = parameters['scope']


# COMMAND ----------

connection_string = dbutils.secrets.get(scope, key=connection_string_key)
blob_acc_url = f"https://{storage_account}.blob.core.windows.net"

# COMMAND ----------


try:
  df_etag = spark.sql(f"SELECT * FROM {env}_genai_configuration.genai_audit_config.evaluationtag").toPandas()
  standard_etag = spark.sql(f"SELECT * FROM {env}_genai_configuration.genai_audit_config.evaluationtag WHERE isStandardCallAudit = true").toPandas()
except Exception as e:
  print(e)
  df_etag = pd.DataFrame(columns=['EvaluationTag_Id','EvaluationTag_Name', 'created_date','updated_date','createdBy','isStandardCallAudit'])
  standard_etag = pd.DataFrame(columns=['EvaluationTag_Id','EvaluationTag_Name', 'created_date','updated_date','createdBy','isStandardCallAudit'])

# COMMAND ----------

blob = BlobClient(
    account_url=blob_acc_url,
    container_name=container,
    blob_name=file_path,
    credential=connection_string
)

# Download the blob content into a BytesIO object
blob_data = blob.download_blob().readall()
blob_stream = BytesIO(blob_data)
print(file_path)
print(blob_stream)

# COMMAND ----------

# Read the Excel file from the BytesIO object
auditscenarios_df = pd.read_excel(blob_stream)
auditscenarios_df

# COMMAND ----------

replace_dict = {'/':'(or)', '.':'', '…': ''}

# COMMAND ----------

from functools import reduce
from datetime import datetime, timedelta,date,time

# Apply the replacement function to the 'question' and 'criteria' column using a dictionary
def replace_fun(column, replace_dict):
    auditscenarios_df[column] = auditscenarios_df[column].apply(
        lambda x: 
        reduce(lambda s, kv: s.replace(*kv), replace_dict.items(), x) 
        if isinstance(x, str) else x
    )
    auditscenarios_df[column] = auditscenarios_df[column].str.lower()
    return auditscenarios_df[column]
auditscenarios_df['question'] = replace_fun('question', replace_dict)
auditscenarios_df['Criteria'] = replace_fun('Criteria', replace_dict)
auditscenarios_df['updated_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#auditscenarios_df['EvaluationTag_Id'] = None

# Display the updated DataFrame
display(auditscenarios_df)

# COMMAND ----------

eval_tags_list = list(df_etag['EvaluationTag_Name'])
eval_tag_ids_list = list(df_etag['EvaluationTag_Id'])
isStandardCallAudit_list = list(df_etag['isStandardCallAudit']) 
standard_tags_list = list(standard_etag['EvaluationTag_Name'])

# COMMAND ----------

#Check if Evaluation Tags in current Audit scenarios table are already there in Evaluation Tags Table, if exist skip them.
auditscenarios_df = auditscenarios_df[~auditscenarios_df['evaluation_tag'].isin(eval_tags_list)]
print('Length of updated audit scenarios tags: ',len(auditscenarios_df))
skipped_auditscenarios_df = auditscenarios_df[auditscenarios_df['evaluation_tag'].isin(eval_tags_list)]
print('Below are skipped audit scenarios, Evaluation tags already present:')
print('Length of skipped audit scenarios tags: ',len(skipped_auditscenarios_df))
skipped_auditscenarios_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### # ## Update Evaluation Tags

# COMMAND ----------

len(eval_tag_ids_list)

# COMMAND ----------

eval_tags_list

# COMMAND ----------

#Get list of eval tags from current Audit scenarios table
curr_etags_list = list(auditscenarios_df['evaluation_tag'])
curr_acc_ids_list = list(auditscenarios_df['account_Id'])
#convert eval tags to lower case
eval_tags_list = [x.lower() for x in eval_tags_list]
curr_etags_list = [x.lower() for x in curr_etags_list]

#check if eval_tags not exist in standard eval tags list
updated_etags_list = [x for x in curr_etags_list if x not in eval_tags_list]
updated_etags_list = curr_etags_list
curr_isStandardCallAudit = [True if x == str(std_acc_id) else False for x in curr_acc_ids_list]

#get start and end numbers of eval tag Sl.No's
etag_sl_no_start = 1 if len(eval_tag_ids_list)== 0 else eval_tag_ids_list[-1]+1
print('Sl. No. start: ', etag_sl_no_start)
print('Length of updated eval tags: ',len(updated_etags_list))
etag_sl_no_end = len(updated_etags_list) + etag_sl_no_start
print('Sl. No. end: ', etag_sl_no_end)

#Prepare updated Sl.No's list
latest_sl_no_list = [i for i in range(etag_sl_no_start, etag_sl_no_end)]
updated_sl_no_list = eval_tag_ids_list + latest_sl_no_list

#Prepare updated eval tags list
updated_eval_tags_list = eval_tags_list + updated_etags_list

#Prepare updated isStandardCallAudit list
updated_isStandardCallAudit = isStandardCallAudit_list + curr_isStandardCallAudit

print(len(updated_sl_no_list))
print(len(updated_eval_tags_list))
print(len(updated_isStandardCallAudit))

# COMMAND ----------

print(len(updated_sl_no_list))
print(len(updated_eval_tags_list))
print(len(updated_isStandardCallAudit))

# COMMAND ----------

#create Evaluation Tag's Dataframe
updated_eval_tag_table = pd.DataFrame({'EvaluationTag_Id': updated_sl_no_list, 'EvaluationTag_Name': updated_eval_tags_list, 'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'updated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'createdBy': 'DIF_Team', 'isStandardCallAudit': updated_isStandardCallAudit})

# COMMAND ----------

updated_eval_tag_table.tail(5)

# COMMAND ----------

updated_eval_tag_table['EvaluationTag_Id'] = updated_eval_tag_table['EvaluationTag_Id'].astype(int)
updated_eval_tag_table['created_date'] = pd.to_datetime(updated_eval_tag_table['created_date'])
updated_eval_tag_table['updated_date'] = pd.to_datetime(updated_eval_tag_table['updated_date'])

# COMMAND ----------

from pyspark.sql.functions import *
def etag_dbWrite(dbParameters,tableName,tableContents):
  sparkDF = spark.createDataFrame(tableContents)
  sparkDF = sparkDF.withColumn("EvaluationTag_Id",col("EvaluationTag_Id").cast("int"))
  sparkDF = sparkDF.withColumn("updated_date",to_timestamp("updated_date").cast("timestamp"))
  sparkDF = sparkDF.withColumn("created_date",to_timestamp("created_date").cast("timestamp"))
  sparkDF = sparkDF.withColumn("isStandardCallAudit",col("isStandardCallAudit").cast("boolean"))
  sparkDF.write.format("delta").option("mergeSchema", "true").mode("overwrite").option("delta.columnMapping.mode", "name").option('delta.minReaderVersion', '2').option('delta.minWriterVersion', '5').save(adlslocation)
  spark.sql("CREATE TABLE if not exists {}.{} USING DELTA LOCATION '{}' WITH (CREDENTIAL `difscaedifdunitycatalog`) ".format(dbParameters['database'],tableName,adlslocation))

# COMMAND ----------

from datetime import datetime, timedelta,date,time
dbParameters = dict()
dbParameters['database'] = f'{env}_genai_configuration.genai_audit_config'
#container name is qa-genai-audit-config ---> for qa
dbParameters['adlsLocation'] = f'abfss://{out_container}@{out_storage_account}.dfs.core.windows.net/config/'

# COMMAND ----------

tableName = 'evaluationtag'
adlslocation = dbParameters['adlsLocation'] + tableName + "/"
etag_dbWrite(dbParameters, tableName, updated_eval_tag_table)

# COMMAND ----------

from pyspark.sql.functions import *
def auditscenarios_dbWrite(dbParameters,tableName,tableContents):
  sparkDF = spark.createDataFrame(tableContents)
  sparkDF = sparkDF.withColumn("audit_Scenario_id",col("audit_Scenario_id").cast("int"))
  sparkDF = sparkDF.withColumn("Explanation",col("Explanation").cast("string"))
  sparkDF = sparkDF.withColumn("updated_date",to_timestamp("updated_date").cast("timestamp"))
  sparkDF = sparkDF.withColumn("created_date",to_timestamp("created_date").cast("timestamp"))
  sparkDF = sparkDF.withColumn("is_enabled",col("is_enabled").cast("boolean"))
  sparkDF = sparkDF.withColumn("is_unauth_eval_tag",col("is_unauth_eval_tag").cast("boolean"))
  #sparkDF = sparkDF.withColumn("EvaluationTag_Id",col("EvaluationTag_Id").cast("bigint"))
  sparkDF.write.format("delta").option("mergeSchema", "true").mode("append").option("delta.columnMapping.mode", "name").option('delta.minReaderVersion', '2').option('delta.minWriterVersion', '5').save(adlslocation)
  spark.sql("CREATE TABLE if not exists {}.{} USING DELTA LOCATION '{}' WITH (CREDENTIAL `difscaedifdunitycatalog`) ".format(dbParameters['database'],tableName,adlslocation))

# COMMAND ----------

tableName = 'auditscenarios'
adlslocation = dbParameters['adlsLocation'] + tableName + "/"
auditscenarios_dbWrite(dbParameters, tableName, auditscenarios_df)
