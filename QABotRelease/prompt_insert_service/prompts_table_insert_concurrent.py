# Databricks notebook source
# DBTITLE 1,import libraries
import json
import uuid
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,getting notebook parameters
try:
  dbutils.widgets.text("notebook_parameter","","") #getting values from parent Notebook through 
  params = dbutils.widgets.get("notebook_parameter") #getting all values in strings
  
  #Convert string values into dict for using it in locals() function (Unpacking purpose)
  param = json.loads(params)
  print(param)

  #fetch all the values from dict without iteration (Can call any value through key)
  locals().update(param)

  print(param)

except Exception as e:
  print(e)
  raise e

# COMMAND ----------

# DBTITLE 1,notebook output- initialization
nb_status={
  "file_path": file_path,
  "value": value,
  "prompts_table_insert_status": '',
  "error_message": ''
}

#dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,variables
yearmonthdate = datetime.now().strftime('%Y%m%d')
#print(yearmonthdate)

archive_folder = "abfss://" + reference_container + "@" + storage_account_name + ".dfs.core.windows.net/prompts_source/" + account_name +"/"+  verification_type + "_prompts_archive/" + yearmonthdate + "/"
print("archive_folder:", archive_folder)

uu_id = uuid.uuid4()
print("uuid:", uu_id)

account_id_result = spark.sql("""select distinct account_id from {0}.{1}.accounts where lower(accountname)=lower('{2}')""".format(config_catalog_name, config_schema_name, account_name)).first()
if account_id_result:
  account_id = account_id_result[0]
else:
  account_id = -1
print('account_id:', account_id)

prompt_archive_component_id_result = spark.sql("""select distinct component_id from {0}.{1}.component where lower(component_name)='prompt adls file archive'""".format(config_catalog_name, config_schema_name)).first()
if prompt_archive_component_id_result:
  prompt_archive_component_id = prompt_archive_component_id_result[0]
else:
  prompt_archive_component_id = -1
print('prompt_archive_component_id:', prompt_archive_component_id)

prompt_insert_component_id_result = spark.sql("""select distinct component_id from {0}.{1}.component where lower(component_name)='prompt table insert'""".format(config_catalog_name, config_schema_name)).first()
if prompt_insert_component_id_result:
  prompt_insert_component_id = prompt_insert_component_id_result[0]
else:
  prompt_insert_component_id = -1
print('prompt_insert_component_id:', prompt_insert_component_id)

# COMMAND ----------

# DBTITLE 1,user defined functions
def audit_error_logging(component_id, file_name, status, error):
  try:
    updated_error = str(error).replace('\'','').lower()
    spark.sql("""insert into {0}.{1}.log (log_id, account_id, component_id, guid, file_name, status, details, created_date, updated_date)
select '{4}' as log_id,
'{2}' as  account_id,
{3} as component_id,
'{4}' as guid,
'{5}' as file_name,
'{6}' as status,
case when isnull('{7}') then null else '{7}' end as details,
current_timestamp(),
current_timestamp()""".format(config_catalog_name, config_schema_name, account_id, component_id, uu_id, file_name, status, updated_error))
    #print('log data inserted successfully')
  except Exception as e:
    print('error while inserting data into log table')
    print(e)


def create_archive_folder(archive_folder):
  try:
    dbutils.fs.ls(archive_folder)
    print("archive folder exists")
  except Exception as e:
    if 'The specified path does not exist' in str(e):
      print("folder does not exist and hence creating the today's date folder inside archive folder")
      try:
        dbutils.fs.mkdirs(archive_folder)
        print("created folder with today's date inside archive folder")
      except Exception as e:
        print(str(e))
        raise Exception(e)


def archive_prompt_file(src_file, tgt_dir):
  try:
    print("src_file: ", src_file)
    print("tgt_dir:", tgt_dir)
    dbutils.fs.cp(src_file, tgt_dir)
    print('source file copied to archive folder')
    try:
      dbutils.fs.rm(src_file)
      audit_error_logging(prompt_archive_component_id, file_path, 'Succeeded', 'file archived')
    except Exception as e:
      print('error while removing source file from prompts source folder')
      print(str(e))
      audit_error_logging(prompt_archive_component_id, file_path, 'Failed', e)
  except Exception as e:
    print('error while copying source file to archive folder')
    print(str(e))
    audit_error_logging(prompt_archive_component_id, file_path, 'Failed', e)
  

# COMMAND ----------

# DBTITLE 1,prompt file exists check
try:
  dbutils.fs.ls(file_path)
  print("file_path exists")
except Exception as e:
  audit_error_logging(prompt_insert_component_id, file_path, 'Failed', e)
  nb_status['prompts_table_insert_status'] = 'Failed'
  nb_status['error_message'] = 'prompt file does not exist'
  dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,word count check
#text = """please give "yes" or "no". This is an example sentence."""
word_count = len(value.split())
print("word_count: ", word_count)
if word_count > 10000:
  msg = "The given prompt is too long. Please check the prompt. Expected length is 10000 words."
  err_archive_flag = 1
  audit_error_logging(prompt_insert_component_id, file_path, 'Failed', msg)
  #for error files archive should not happen
  #create_archive_folder(archive_folder)
  #archive_prompt_file(file_path, archive_folder)
  nb_status['prompts_table_insert_status'] = 'Failed'
  nb_status['error_message'] = msg
else:
  err_archive_flag = 0
  msg = "The given prompt is in expected format"
print(msg)

# COMMAND ----------

# DBTITLE 1,exiting notebook for word count check
#print(nb_status)
if err_archive_flag == 1:
  dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,prompt format check
#err_archive_flag = 0
actual_prompt = value
j_prompt = '{ ' + value.replace('{', '').replace('}', '') + ' }'
try:
  dic_prompt = eval(j_prompt)
  #print(dic_prompt)
  actual_key_list = list(dic_prompt.keys())
  print('actual_key_list:', actual_key_list)
  key_list_lower_str = str(actual_key_list).replace('[','').replace(']','').replace('\'','').lower()
  print('key_list_lower_str:', key_list_lower_str)
  key_list = list(key_list_lower_str.split(', '))
  print('key_list:', key_list)

  if key_list:
    msg = '''given prompt is in expected format'''
    print(msg)
    str_key_list = str(key_list)
    key = str_key_list.replace('[', '').replace(']', '').lower()
    print('key:', key)
  else:
    err_archive_flag = 1
    msg = '''There is no key in the given prompt. Please check the prompt. Expected format is: "key1": "value1", "key2": "value2", ...'''
    print(msg)

except Exception as e:
  print(e)
  err_archive_flag = 1
  msg = '''given prompt is not in expected format. Please check the prompt. Expected format is: "key1": "value1", "key2": "value2", ...'''
  print(msg)

if err_archive_flag == 1:
  audit_error_logging(prompt_insert_component_id, file_path, 'Failed', msg)
  #for error files archive should not happen
  #create_archive_folder(archive_folder)
  #archive_prompt_file(file_path, archive_folder)
  nb_status['prompts_table_insert_status'] = 'Failed'
  nb_status['error_message'] = msg

# COMMAND ----------

# DBTITLE 1,exiting notebook for prompts format failure
#print(nb_status)
if err_archive_flag == 1:
  dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,evaluationTag, auditScenarios and prompts table check
# 1. remove {,} brackets from the prompt string
# 2. remove empty spaces at the end
# 3. add {,} brackets at the start and end of the prompt string respectively
err_archive_list = []

try:
  #evaluationTag table check
  evaluation_tbl_chk_df = spark.sql("""select * from {0}.{1}.evaluationtag where lower(EvaluationTag_Name) in ({2})
  """.format(config_catalog_name, config_schema_name, key))
  #display(evaluation_tbl_chk_df)
  if evaluation_tbl_chk_df.count() == len(key_list):
    msg = 'evaluationTag table has evaluation tags given in the prompt'
    print(msg)

    #auditscenerios table check
    audit_scenerios_tbl_chk_df = spark.sql("""select * from {0}.{1}.auditscenarios where account_id in ('{4}') and lower(verificationType) = lower('{3}') and lower(Evaluation_Tag) in ({2})""".format(config_catalog_name, config_schema_name, key, verification_type, account_id))

    if audit_scenerios_tbl_chk_df.count() == len(key_list):
      msg = 'auditScenerios table has evaluation tags given in the prompt for its respective verification type'
      print(msg)

      #prompts table check
      prompt_tbl_chk_df = spark.sql("""select prompts_id, account_id, prompts, isenable, verification_group from {0}.{1}.prompts where Account_Id in ('{2}') and lower(IsEnable) = 'true' and lower(verification_group) = lower('{3}')""".format(config_catalog_name, config_schema_name, account_id, verification_type))

      if prompt_tbl_chk_df.count() > 0:
        for i in prompt_tbl_chk_df.collect():
          existing_prompt = i['prompts']
          j_existing_prompt = '{ ' + existing_prompt.replace('{', '').replace('}', '') + ' }'
          dic_existing_prompt = eval(j_existing_prompt)
          key_list_existing_prompt_list = list(dic_existing_prompt.keys())
          key_list_existing_prompt_lower_str = str(key_list_existing_prompt_list).replace('[','').replace(']','').replace('\'','').lower()
          print('key_list_existing_prompt_lower_str:', key_list_existing_prompt_lower_str)
          key_list_existing_prompt = list(key_list_existing_prompt_lower_str.split(', '))
          print('key_list_existing_prompt:', key_list_existing_prompt)
          matching_list = list(set(key_list).intersection(key_list_existing_prompt))
          print('matching list: ', matching_list)
          if len(matching_list) > 0:
            err_archive_list.append(1)
            msg = 'prompt table already has active prompt with evaluation tags given in this prompt'
            print(msg)
          else:
            err_archive_list.append(0)
    
      else: #prompts table
        err_archive_list.append(0)
        msg = 'prompt table does not have active prompt with evaluation tags given in this prompt'
        print(msg)

    else: #auditscenerios table
      err_archive_list.append(1)
      msg = 'auditscenarios table does not have the evaluation tags given in the prompt for its respective verification_type'
      print(msg)
      
  else: #evaluationtag table
    err_archive_list.append(1)
    msg = 'evaluationTag table does not have the evaluation tags given in the prompt'
    print(msg)

except Exception as e:
  print(e)
  err_archive_list.append(1)
  msg = '''given prompt is not in expected format. Please check. Expected format is: "key1": "value1", "key2": "value2", ...'''
  print(msg)

set_dic = set(err_archive_list)
print('error_archive_flag_dictionary:', set_dic)

#if err_archive_flag >= 1:
if 1 in set_dic:
  audit_error_logging(prompt_insert_component_id, file_path, 'Failed', msg)
  #for error files archive should not happen
  #create_archive_folder(archive_folder)
  #archive_prompt_file(file_path, archive_folder)
  nb_status['prompts_table_insert_status'] = 'Failed'
  nb_status['error_message'] = msg

# COMMAND ----------

# DBTITLE 1,exiting notebook for evaluation tags check failure
#if err_archive_flag == 1:
if 1 in set_dic:
  dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,prompts table insert
try:
  max_prompt_id = spark.sql("select max(prompts_id)+1 from {0}.{1}.prompts".format(config_catalog_name, config_schema_name)).first()[0]
  if(max_prompt_id == None):
    max_prompt_id = 1

  spark.sql("""insert into {0}.{1}.prompts values
  ({2}, 
  '{3}', 
  '{4}',
  '''{5}''', 
  True,
  current_timestamp(), 
  current_timestamp(), 
  'DIF_Team')
  """.format(config_catalog_name, config_schema_name, max_prompt_id, account_id, verification_type, value))
  msg = "prompts table insert completed for prompts_id: " + str(max_prompt_id)
  print(msg)

  audit_error_logging(prompt_insert_component_id, file_path, 'Succeeded', msg)
  nb_status['prompts_table_insert_status'] = 'Succeeded'
  nb_status['error_message'] = msg
  p_ar_flag = 1

except Exception as e:
  print(str(e))
  audit_error_logging(prompt_insert_component_id, file_path, 'Failed', e)
  msg = "prompts table insert failed. Please check log for more details"
  nb_status['prompts_table_insert_status'] = 'Failed'
  nb_status['error_message'] = msg
  p_ar_flag = 0

# COMMAND ----------

# DBTITLE 1,archiving prompt file
#if archive_flag == 1:
if 1 not in set_dic and p_ar_flag == 1:
  create_archive_folder(archive_folder)
  archive_prompt_file(file_path, archive_folder)
else:
  print('file is not archived')

# COMMAND ----------

# DBTITLE 1,notebook exit
dbutils.notebook.exit(json.dumps(nb_status))
