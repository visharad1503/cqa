# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,parameters
# dbutils.widgets.text("account_name", "")
# dbutils.widgets.text("config_catalog_name", "")
# dbutils.widgets.text("config_schema_name", "")
# dbutils.widgets.text("stage_catalog_name", "dev_genai_stg")
# dbutils.widgets.text("pii_catalog_name", "dev_genai")
# dbutils.widgets.text("storage_account_name", "")
# dbutils.widgets.text("stage_container_name", "dev-genai-stg")
# dbutils.widgets.text("prompts_id_json", "")
# dbutils.widgets.text("script_guid", "")
# dbutils.widgets.text("prompts_result", "")

dbutils.widgets.text("account_name", "")
dbutils.widgets.text("config_catalog_name", "")
dbutils.widgets.text("config_schema_name", "")
dbutils.widgets.text("stage_catalog_name", "")
dbutils.widgets.text("pii_catalog_name", "")
dbutils.widgets.text("storage_account_name", "")
dbutils.widgets.text("stage_container_name", "")
dbutils.widgets.text("prompts_id_json", '''''')
dbutils.widgets.text("script_guid", "")
dbutils.widgets.text("prompts_result", ''' ''')
dbutils.widgets.text("prompts_result","")


account_name = dbutils.widgets.get("account_name")
config_catalog_name = dbutils.widgets.get("config_catalog_name")
config_schema_name = dbutils.widgets.get("config_schema_name")
stage_catalog_name = dbutils.widgets.get("stage_catalog_name")
pii_catalog_name = dbutils.widgets.get("pii_catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
stage_container_name = dbutils.widgets.get("stage_container_name")
prompts_id_json = dbutils.widgets.get("prompts_id_json")
script_guid = dbutils.widgets.get("script_guid")
prompts_result = dbutils.widgets.get("prompts_result")

# COMMAND ----------

# DBTITLE 1,importing headers
import json
import uuid
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,notebook status flag
nb_status = {
  "script_guid": script_guid,
  "error_flag": "0"
}

# COMMAND ----------

# DBTITLE 1,variables
config_container_name = 'genai-audit-config'
config_folder_name = 'config'
stage_schema_name = account_name + "_transcriptor"
monthyear = datetime.now().strftime('%m%Y')
pii_table_name = 'piiscrubbed_' + monthyear

#uu_id = uuid.uuid4()
uu_id = script_guid
print("uuid:", uu_id)

account_id_result = spark.sql("""select distinct account_id from {0}.{1}.accounts where lower(accountname)=lower('{2}')""".format(config_catalog_name, config_schema_name, account_name)).first()
if account_id_result:
  account_id = account_id_result[0]
else:
  account_id = -1
print('account_id:', account_id)

prompt_result_component_id_result = spark.sql("""select distinct component_id from {0}.{1}.component where lower(component_name)='promptsresults table insert'""".format(config_catalog_name, config_schema_name)).first()
if prompt_result_component_id_result:
  prompt_result_component_id = prompt_result_component_id_result[0]
else:
  prompt_result_component_id = -1
print('prompt_result_component_id:', prompt_result_component_id)

# COMMAND ----------

# DBTITLE 1,user defined functions
def audit_error_logging(component_id, file_name, status, error):
  updated_error = str(error).replace('\'','').lower()
  try:
    spark.sql("""insert into {0}.{1}.log (log_id, account_id, component_id, guid, file_name, status, details, created_date, updated_date)
select '{4}' as log_id,
'{2}' as  account_id,
{3} as component_id,
'{4}' as guid,
'{5}' as file_name,
'{6}' as status,
case when isnull('''{7}''') then null else ('''{7}''') end as details,
current_timestamp(),
current_timestamp()""".format(config_catalog_name, config_schema_name, account_id, component_id, uu_id, file_name, status, updated_error))
    #print('log data inserted successfully')
  except Exception as e:
    print('error while inserting data into log table')
    print(e)

# COMMAND ----------

# DBTITLE 1,dictionary conversion
#prompts_id_json = '{}'
dic_prompt_id = eval(prompts_id_json)
print(dic_prompt_id)

#prompts_result = '{}'
dic_prompt_result = eval(prompts_result)
print(dic_prompt_result)

# COMMAND ----------

# DBTITLE 1,not null check
if dic_prompt_result and dic_prompt_id:
  print('prompts_id_json and prompts_result are not empty')
else:
  print('Either prompts_id_json or prompts_result are having empty values. please check.')
  nb_status["error_flag"] = "1"
  dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,promptsresults table creation
spark.sql("""CREATE TABLE if not exists {0}.{1}.promptsresults (
  Prompts_Result_Id String,
  Account_Id STRING,
  Prompts_Id_Json STRING,
  Prompts_Results STRING,
  Script_GUID STRING,
  IsEnabled BOOLEAN,
  Is_Splitted BOOLEAN,
  Created_Date TIMESTAMP,
  Updated_Date TIMESTAMP,
  CreatedBy STRING)
USING DELTA
OPTIONS (
  path 'abfss://{2}@{3}.dfs.core.windows.net/{4}/promptsresults',
  credential 'difscaedifdunitycatalog'
)
PARTITIONED BY (Script_GUID)
""".format(config_catalog_name, config_schema_name, config_container_name, storage_account_name, config_folder_name))

# COMMAND ----------

# DBTITLE 1,promptsresults table insert
try:
  tgt_count_df = spark.sql("""select * from {0}.{1}.promptsresults where script_guid = '{2}' and Prompts_Id_Json in ('{3}') and Account_Id = '{4}'""". format(config_catalog_name, config_schema_name, script_guid, prompts_id_json, account_id))
  print(tgt_count_df.count())

  if tgt_count_df.count() == 0:
    prompts_result_id = uuid.uuid4()
    print (prompts_result_id)
    spark.sql("""insert into {0}.{1}.promptsresults
(Prompts_Result_Id, Account_Id, Prompts_Id_Json, Prompts_Results, script_GUID, IsEnabled, Is_Splitted, Created_Date, Updated_Date, CreatedBy)
select 
'{2}',
'{3}',
'{4}', 
'''{5}''',
'{6}',
True, 
False, 
current_timestamp(), 
current_timestamp(), 
'DIF_Team'
""".format(config_catalog_name, config_schema_name, prompts_result_id, account_id, prompts_id_json, prompts_result.replace("'",'"'), script_guid))
    msg = "promptsresults table insert completed for prompts_result_id: " + str(prompts_result_id)
    print(msg)

  else:
    spark.sql("""update {0}.{1}.promptsresults set 
Prompts_Results = '''{4}''',
IsEnabled = True,
Is_Splitted = False,
Updated_Date = current_timestamp()
where Script_GUID = '{5}'
""".format(config_catalog_name, config_schema_name, account_id, prompts_id_json, prompts_result.replace("'",'"'), script_guid))
    msg = "promptsresults table update completed"
    print(msg)
    
except Exception as e:
  msg = "Data load to promptsresults table got failed: " + str(e)
  audit_error_logging(prompt_result_component_id, script_guid, 'Failed', msg)
  print(msg)
  nb_status["error_flag"] = "1"

# COMMAND ----------

# DBTITLE 1,account_qualityaudit_result table creation
spark.sql("""CREATE TABLE if not exists {0}.{1}.account_qualityaudit_result (
  Account_QualityAudit_Result_Id String,
  Account_Id STRING,
  Script_GUID STRING,
  Audit_Scenario_id INT,
  Prompts_Id INT,
  Verification_Type STRING,
  Evaluation_Tag_Id INT,
  Evaluation_Tag_Name STRING,
  Result_Status STRING,
  Result_Summary STRING,
  IsEnabled BOOLEAN,
  IsSplitComplteted BOOLEAN,
  Created_Date TIMESTAMP,
  Updated_Date TIMESTAMP,
  CreatedBy STRING)
USING DELTA
OPTIONS (
  path 'abfss://{2}@{3}.dfs.core.windows.net/{4}/account_qualityaudit_result',
  credential 'difscaedifdunitycatalog'
)
PARTITIONED BY (Script_GUID)
""".format(stage_catalog_name, stage_schema_name, stage_container_name, storage_account_name, account_name))

# COMMAND ----------

# DBTITLE 1,account_qualityaudit_result table insert
try:
  # to get prompts_ids
  #dic_prompt_id = eval(prompts_id_json)
  print(str(dic_prompt_id))
  agent_prompts_id = dic_prompt_id['agent_prompts_id']
  if agent_prompts_id == '':
    agent_prompts_id = 0
  print("agent_prompts_id: ", agent_prompts_id)
  customer_prompts_id = dic_prompt_id['customer_prompts_id']
  if customer_prompts_id == '':
    customer_prompts_id = 0
  print("customer_prompts_id: ", customer_prompts_id)
  prompts_ids = str(agent_prompts_id) + ',' + str(customer_prompts_id)
  print("all prompts_ids: ", prompts_ids)

  # to get evaluationtags
  #dic_prompt_result = eval(prompts_result)
  print(dic_prompt_result)
  given_evaluationtags_list = list(dic_prompt_result.keys())
  given_evaluationtags = str(given_evaluationtags_list).replace('[', '').replace(']', '').lower()
  print('given_evaluationtags:', given_evaluationtags)
 
  for each_evaluationtag in dic_prompt_result.keys():
    print("evaluationtag: ", each_evaluationtag)
    res = dic_prompt_result[each_evaluationtag]['result']
    print(res)
    res_dict = eval(res)
    status = ""
    summary = ""
    for i in res_dict.keys():
      print(i)
      if 'status' in i.lower():
        status = res_dict[i].strip()
        print(status)
      if 'summary' in i.lower():
        summary = res_dict[i].strip()
        print(summary)

    #print(account_id, prompts_ids, each_evaluationtag.lower(), summary, status, script_guid, config_catalog_name, config_schema_name)
    tgt_count_df = spark.sql("""select * from {0}.{1}.account_qualityaudit_result where script_guid = '{2}' and lower(Evaluation_Tag_Name) in ('{3}') and Prompts_Id in ({4})""". format(stage_catalog_name, stage_schema_name, script_guid, each_evaluationtag.lower(), prompts_ids))
    print(tgt_count_df.count())

    if tgt_count_df.count() == 0:
      spark.sql("""insert into {8}.{9}.account_qualityaudit_result
(account_qualityaudit_result_id, Account_Id, Script_GUID, Audit_Scenario_id, Prompts_Id, Verification_Type, Evaluation_Tag_Id, Evaluation_Tag_Name, Result_Status, Result_Summary, IsEnabled, IsSplitComplteted, Created_Date, Updated_Date, CreatedBy)
select uuid() as account_qualityaudit_result_id, 
                 a.Account_Id as Account_Id,
                 '{5}' as Script_GUID,
                a.Audit_Scenario_id as Audit_Scenario_id,
                p.Prompts_Id as Prompts_Id,
                a.VerificationType as Verification_Type,
                t.EvaluationTag_Id as Evaluation_Tag_Id,
                a.Evaluation_Tag as Evaluation_Tag_Name,
                '{3}' as Result_Status,
                '{4}' as Result_Summary,
                True as IsEnabled,
                True as IsSplitComplteted,
                current_timestamp() as Created_Date, 
                current_timestamp() as Updated_Date,
                'DIF_Team' as CreatedBy
                from {6}.{7}.auditscenarios a 
                inner join {6}.{7}.evaluationTag t
                on trim(lower(t.EvaluationTag_Name)) = trim(lower('{2}'))
                inner join {6}.{7}.prompts p
                on a.account_id = p.account_id and trim(lower(a.verificationType)) = trim(lower(p.verification_group)) and trim(lower(a.Evaluation_Tag)) = trim(lower('{2}')) and trim(lower(p.prompts)) like ('%{2}%')
                where a.account_id = '{0}' and p.prompts_id in ({1})
""".format(account_id, prompts_ids, each_evaluationtag.lower(), status, summary, script_guid, config_catalog_name, config_schema_name, stage_catalog_name, stage_schema_name))
      msg = "account_qualityaudit_result table insert completed"
      print(msg)

    else:
      spark.sql("""update {0}.{1}.account_qualityaudit_result set 
Result_Summary = '''{2}''',
Result_Status = '{3}',
IsEnabled = True,
Updated_Date = current_timestamp()
where Script_GUID = '{4}' and lower(Evaluation_Tag_Name) in ('{5}') and Prompts_Id in ({6})
""".format(stage_catalog_name, stage_schema_name, summary, status, script_guid, each_evaluationtag.lower(), prompts_ids))
      msg = "account_qualityaudit_result table update completed"
      print(msg)
    
except Exception as e:
  msg = "Data load to account_qualityaudit_result table got failed: " + str(e)
  audit_error_logging(prompt_result_component_id, script_guid, 'Failed', msg)
  print(msg)
  nb_status["error_flag"] = "1"

# COMMAND ----------

# DBTITLE 1,promptsresults table Is_Splitted update
try:
  spark.sql("""update {0}.{1}.promptsresults  
set Is_Splitted = True
where Account_Id = '{2}' and Script_GUID = '{3}'""".format(config_catalog_name, config_schema_name, account_id, script_guid))
  msg = "promptsresults table - is_splitted flag update completed"
  print(msg)
  audit_error_logging(prompt_result_component_id, script_guid, 'Succeeded', msg)
  
except Exception as e:
  print(str(e))
  audit_error_logging(prompt_result_component_id, script_guid, 'Failed', e)
  msg = "promptsresults table - is_splitted flag update failed. Please check log for more details"
  print(msg)
  nb_status["error_flag"] = "1"
  #raise Exception(e)

# COMMAND ----------

# DBTITLE 1,piiscrub table is_qacompleted flag update
try:
  spark.sql("""update {0}.{1}.{2} set is_qacompleted = 'true' where Script_GUID = '{3}';
""".format(pii_catalog_name, stage_schema_name, pii_table_name, script_guid))
  msg = "piiscrub table - is_qacompleted flag update completed"
  print(msg)
  audit_error_logging(prompt_result_component_id, script_guid, 'Succeeded', msg)

except Exception as e:
  print(str(e))
  audit_error_logging(prompt_result_component_id, script_guid, 'Failed', e)
  msg = "promptsresults table - is_qacompleted flag update failed. Please check log for more details"
  print(msg)
  nb_status["error_flag"] = "1"
  #raise Exception(e)

# COMMAND ----------

# 1. Find out the evalutaion tag having isauthorizeet

# COMMAND ----------

# DBTITLE 1,exiting notebook with output
dbutils.notebook.exit(json.dumps(nb_status))
