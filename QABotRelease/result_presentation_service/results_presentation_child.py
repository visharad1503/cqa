# Databricks notebook source
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

adls_storage_account = parameters["adls_storage_account_name"]
out_catalog = parameters["output_catalog_name"]
out_schema = parameters["output_schema_name"]
out_container = parameters["out_container"]
config_catalog_name = parameters["config_catalog_name"]
config_schema_name = parameters["config_schema_name"]
stg_catalog_name = parameters["stg_catalog_name"]
stg_schema_name = parameters["stg_schema_name"]
final_catalog_name =parameters["final_catalog_name"] 
final_schema_name = parameters["final_schema_name"]

out_container=f"{env}-{out_container}"
out_catalog=f"{env}_{out_catalog}"
config_catalog_name=f"{env}_{config_catalog_name}"
stg_catalog_name=f"{env}_{stg_catalog_name}"
final_catalog_name=f"{env}_{final_catalog_name}"

# COMMAND ----------

# DBTITLE 1,Accounts Table
accounts_df = spark.sql(f"select * from {config_catalog_name}.{config_schema_name}.accounts").toPandas()

# COMMAND ----------

# DBTITLE 1,Evaluation Tags Table
evaluationtag_df = spark.sql(f"select * from {config_catalog_name}.{config_schema_name}.evaluationtag").toPandas()

# COMMAND ----------

# DBTITLE 1,Audit Scenarios
audit_scenarios_df = spark.sql(f"select * from {config_catalog_name}.{config_schema_name}.auditscenarios").toPandas()

# COMMAND ----------

# DBTITLE 1,Audit Quality - Stg Table
audit_quality_df = spark.sql(f"""SELECT * FROM {stg_catalog_name}.{stg_schema_name}.account_qualityaudit_result WHERE       Script_GUID IN (
    SELECT Script_GUID FROM {stg_catalog_name}.{stg_schema_name}.account_qualityaudit_result GROUP BY Script_GUID 
    HAVING COUNT(*) >= 45)""").toPandas()

# COMMAND ----------

# DBTITLE 1,Presentation Table
import pandas as pd
try:
  audit_qualitybyaccount_df = spark.sql(f"select * from {final_catalog_name}.{final_schema_name}.auditresultsbyaccount order by 'SL.NO' asc").toPandas()
except Exception as e:
  print(e)
  audit_qualitybyaccount_df = pd.DataFrame(columns = ['SL.NO', 'Script_GUID', 'AccountName', 'EvaluationTag', 'isUnauthenticatedScript', 'is_unauth_eval_tag', 'Category', 'Source', 'Criticality', 'Question', 'Criteria', 'VerificationType', 'ResultStatus', 'ResultSummary', 'ExecutionDate'])

# COMMAND ----------

# DBTITLE 1,Join all Tables to create Final DF
query = f"""SELECT act.accountName as AccountName, aqr.Script_GUID as Script_GUID, asce.audit_Scenario_id, pii.is_unauthenticated as is_unauthenticated_script, asce.evaluation_tag, asce.is_unauth_eval_tag, p.prompts, asce.category, asce.source, asce.criticality, asce.question, asce.Criteria, asce.verificationType, aqr.Result_Status, aqr.Result_Summary FROM {config_catalog_name}.{config_schema_name}.auditscenarios asce INNER JOIN {config_catalog_name}.{config_schema_name}.accounts act ON asce.account_Id = act.account_Id INNER JOIN {stg_catalog_name}.{stg_schema_name}.account_qualityaudit_result aqr ON aqr.account_id = act.account_Id AND aqr.audit_Scenario_id = asce.audit_Scenario_id INNER JOIN {config_catalog_name}.{config_schema_name}.prompts p ON p.prompts_id = aqr.prompts_id INNER JOIN {final_catalog_name}.{final_schema_name}.piiscrubbed_102024 pii ON aqr.Script_GUID = pii.script_guid WHERE aqr.isenabled = 'true' """

#Covert to pandas dataframe
joined_df = spark.sql(query).toPandas()
display(joined_df)

# COMMAND ----------

# DBTITLE 1,Create DF only with req columns
from datetime import datetime
import pandas as pd
tableName = 'auditresultsbyaccount'

# Create output df with required columns
user_df = pd.DataFrame(columns = ['SL.NO', 'Script_GUID', 'AccountName', 'EvaluationTag', 'isUnauthenticatedScript', 'is_unauth_eval_tag', 'Category', 'Source', 'Criticality', 'Question', 'Criteria', 'VerificationType', 'ResultStatus', 'ResultSummary', 'ExecutionDate'])

user_df[['AccountName', 'Script_GUID', 'EvaluationTag', 'isUnauthenticatedScript', 'is_unauth_eval_tag', 'Category', 'Source', 'Criticality', 'Question', 'Criteria', 'VerificationType', 'ResultStatus', 'ResultSummary']] = joined_df[['AccountName', 'Script_GUID', 'evaluation_tag', 'is_unauthenticated_script', 'is_unauth_eval_tag', 'category', 'source', 'criticality', 'question', 'Criteria', 'verificationType', 'Result_Status', 'Result_Summary']]

print(len(user_df))
#check and remove duplicates
existing_auditresults_df = spark.sql(f"SELECT * FROM {out_catalog}.{out_schema}.{tableName}").toPandas()
user_df = pd.concat([existing_auditresults_df, user_df], axis=0)
user_df = user_df.drop_duplicates(subset=['Script_GUID', 'AccountName', 'EvaluationTag', 'isUnauthenticatedScript', 'is_unauth_eval_tag', 'Category', 'Source', 'Criticality', 'Question', 'Criteria', 'VerificationType', 'ResultStatus', 'ResultSummary'], keep="last")
user_df = user_df.reset_index(drop=True)
print(len(user_df))

user_df['SL.NO'] = user_df.index + 1
user_df['ExecutionDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
display(user_df)

# COMMAND ----------

# DBTITLE 1,To Handled Unauthorized Scripts
try:
  final_user_df = pd.DataFrame()
  for script_guid in user_df['Script_GUID'].unique():
    filtered_user_df = user_df[user_df['Script_GUID'] == script_guid]
    is_unauthenticated = filtered_user_df['isUnauthenticatedScript'].iloc[0]
    if is_unauthenticated == 'True':
      filtered_user_df = filtered_user_df[filtered_user_df['is_unauth_eval_tag'] == True]
    elif is_unauthenticated == 'False':
      filtered_user_df = filtered_user_df[filtered_user_df['is_unauth_eval_tag'] == False]
    else:
      print('Invalid value for is_unauthenticated_script')
    
    final_user_df = pd.concat([final_user_df, filtered_user_df], axis=0)
  final_user_df = final_user_df.reset_index(drop=True)
  final_user_df['SL.NO'] = final_user_df.index + 1
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Use Reindexing In case of append mode
# Calculate the Index range based on existing DataFrame values
# start = int(audit_qualitybyaccount_df['SL.NO'].iloc[-1]) + 1
# end = start + len(final_user_df)

# Generate the list of numbers in the range
# index_list = list(range(start, end))
# final_user_df['SL.NO'] = index_list

# COMMAND ----------

# DBTITLE 1,DBwrite-Overwrite Mode
from pyspark.sql.functions import *
def dbWrite(dbParameters,tableName,tableContents):
  sparkDF = spark.createDataFrame(tableContents)
  sparkDF.write.format("delta").option("mergeSchema", "true").mode("append").option("delta.columnMapping.mode", "name").option('delta.minReaderVersion', '2').option('delta.minWriterVersion', '5').save(adlslocation)
  spark.sql("CREATE TABLE if not exists {}.{} USING DELTA LOCATION '{}' WITH (CREDENTIAL ``)".format(dbParameters['database'],tableName,adlslocation))

# COMMAND ----------

# DBTITLE 1,DB Parameters
from datetime import datetime, timedelta,date,time
dbParameters = dict()
dbParameters['database'] = f"{out_catalog}.{out_schema}"
dbParameters['adlsLocation'] = f"abfss://{out_container}@{adls_storage_account}.dfs.core.windows.net/presentation/"
print(dbParameters['adlsLocation'])

# COMMAND ----------

# DBTITLE 1,Final output to DB
adlslocation = dbParameters['adlsLocation'] + tableName + "/"
dbWrite(dbParameters, tableName, final_user_df)
# finishted inserting the vaue in Propt result table.
