# Databricks notebook source
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,pip install libraries
# %pip install langchain langchain-openai langchain-experimental presidio-analyzer presidio-anonymizer spacy Faker

# COMMAND ----------

# MAGIC %pip install langchain

# COMMAND ----------

# MAGIC %pip install langchain-openai

# COMMAND ----------

# MAGIC %pip install langchain-experimental

# COMMAND ----------

# MAGIC %pip install presidio-analyzer

# COMMAND ----------

# MAGIC %pip install presidio-anonymizer

# COMMAND ----------

# MAGIC %pip install spacy

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

# DBTITLE 1,import libraries
import json
import pandas
from datetime import date, datetime
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import RecognizerResult, OperatorConfig
from presidio_analyzer import Pattern, PatternRecognizer
from langchain_experimental.data_anonymizer import PresidioAnonymizer
from typing import List

import gc
from pyspark.sql.types import * #added

# COMMAND ----------

# DBTITLE 1,getting notebook parameters
try:
  dbutils.widgets.text("notebook_parameter", "") #getting values from parent Notebook through 
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
#modified
nb_status={
  "script_guid": Script_GUID,
  "script_agent_piiscrubbed": '',
  "script_customer_piiscrubbed": '',
  "is_agent_piiscrubbed": False,
  "is_customer_piiscrubbed": False,
  "is_unauthenticated": isUnauthenticated,
  "script_createddate": Script_CreationDate,
  "script_piiscrubbeddate": '',
  "is_qacompleted": False,
  "script_agent_analyzer_results": [''],
  "script_agent_piiscrubbed_values": [''],
  "script_customer_analyzer_results": [''],
  "script_customer_piiscrubbed_values": ['']
}

#dbutils.notebook.exit(json.dumps(nb_status))

# COMMAND ----------

# DBTITLE 1,variables
uu_id = Script_GUID
print("uuid:", uu_id)

account_id_result = spark.sql("""select distinct account_id from {0}.{1}.accounts where lower(accountname)=lower('{2}')""".format(config_catalog_name, config_schema_name, account_name)).first()
if account_id_result:
  account_id = account_id_result[0]
else:
  account_id = -1
print('account_id:', account_id)

pii_component_id_result = spark.sql("""select distinct component_id from {0}.{1}.component where lower(component_name)='pii transcript'""".format(config_catalog_name, config_schema_name)).first()
if pii_component_id_result:
  pii_component_id = pii_component_id_result[0]
else:
  pii_component_id = -1
print('pii_component_id:', pii_component_id)

# COMMAND ----------

# DBTITLE 1,user defined functions
#entity_list = ["PERSON", "PHONE_NUMBER", 'EMAIL_ADDRESS', 'IBAN_CODE', 'CREDIT_CARD', 'CRYPTO', 'IP_ADDRESS', 'LOCATION', 'DATE_TIME', 'NRP', 'MEDICAL_LICENSE', 'URL', 'US_BANK_NUMBER', 'US_DRIVER_LICENSE', 'US_ITIN', 'US_PASSPORT', 'US_SSN']
entity_list = ["PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "IP_ADDRESS", "LOCATION", "DATE_TIME", "NRP", "URL", "NUMBER"]
allow_list=["Mm"]


# Analyzer function
def analyzer(transcript, entity_list):
  # Define the regex pattern in a Presidio `Pattern` object:
  #numbers_pattern = Pattern(name="numbers_pattern", regex="(?<!\w)(\(?(\+|00)?48\)?)?[ -]?\d{3}[ -]?\d{3}[ -]?\d{3}(?!\w)", score=0.5)
  numbers_pattern = Pattern(name="numbers_pattern", regex="\d+", score=0.38)
  # Define the recognizer with one or more patterns
  number_recognizer = PatternRecognizer(
    supported_entity="NUMBER", patterns=[numbers_pattern]
  )
  analyzer = AnalyzerEngine()
  analyzer.registry.add_recognizer(number_recognizer)
  #analyzer.add_recognizer(number_recognizer)
  results = analyzer.analyze(text=transcript, entities=entity_list, allow_list=allow_list, language='en')
  print('Analyzer Results: ', results)
  print('Results length: ', len(results))
  if len(results) > 0:
    #print_analyzer_results(results, text=transcript)
    curr_vals = results[0].to_dict()
    entity_type = curr_vals.get('entity_type')
    start = curr_vals.get('start')
    end = curr_vals.get('end')
    score = curr_vals.get('score')
    print('Anonymizer Params: ', entity_type, start, end, score)
    if start > 0:
      print(transcript[(start - 1): end])
      replace_list.append(transcript[(start - 1): end])
    else:
      print(transcript[start: end])
      replace_list.append(transcript[start: end])
  else:
    #print('No Results Found')
    entity_type, start, end, score = 0, 0, 0, 0
  return results, entity_type, start, end, score


# Anonymizer function
def anonymizer(transcript, entity_type, start, end, score):
  engine = AnonymizerEngine()
  anonymize_result = engine.anonymize(
        text=transcript,
        analyzer_results=[
            RecognizerResult(entity_type=entity_type, start=start, end=end, score=score)
        ]
        #,
        #operators={"PERSON": OperatorConfig("replace", {"new_value": "BIP"})},
    )
  return anonymize_result


# Helper method to print results nicely
def print_analyzer_results(results: List[RecognizerResult], text: str):
  a = []
  """Print the results in a human readable way."""
  for i, result in enumerate(results):
    #print(f"Result {i}:")
    print(f" {result}, text: {text[result.start:result.end]}")
    a.append(f" {result}, text: {text[result.start:result.end]}")
    if result.analysis_explanation is not None:
      print(f" {result.analysis_explanation.textual_explanation}")
  return (a)  


# Combining analyzer and anonymizer together for transcript PII scrubbing
def analyzer_anonymizer(transcript):
  analyzer_results, entity_type, start, end, score = analyzer(transcript, entity_list)
  ab = ['']
  if len(analyzer_results) > 0:
    ab = print_analyzer_results(analyzer_results, text=transcript)
    print(ab)
    while len(analyzer_results) > 0:
      anonymizer_result = anonymizer(transcript, entity_type, start, end, score)
      PIIScrubbed_transcript = str(anonymizer_result.text)
      transcript = PIIScrubbed_transcript
      analyzer_results, entity_type, start, end, score = analyzer(transcript, entity_list)
      #print(len(analyzer_results))
  else:
    PIIScrubbed_transcript = transcript
  return PIIScrubbed_transcript,ab

# audit_error_logging function
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

# DBTITLE 1,pii scrubbing - transcript agent
try:
  print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': starting presidio data anonymization for Script_Agent')

  replace_list = []
  print('Script_Agent: ', Script_Agent)
  Script_Agent_PIIScrubbed, Script_Agent_Analyzer_Results = analyzer_anonymizer(Script_Agent)
  print('Script_Agent_PIIScrubbed: ', Script_Agent_PIIScrubbed)

  # Invoke garbage collection
  gc.collect()

  print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': completed presidio data anonymization for Script_Agent')
  nb_status["script_agent_piiscrubbed"] = Script_Agent_PIIScrubbed
  nb_status["is_agent_piiscrubbed"] = True
  nb_status["script_piiscrubbeddate"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  nb_status["script_agent_analyzer_results"] = Script_Agent_Analyzer_Results
  nb_status["script_agent_piiscrubbed_values"] = replace_list

except Exception as e:
  msg = "Unable to PII scrub Script_Agent transcripts: " + str(e)
  nb_status["is_agent_piiscrubbed"] = False
  print(msg)
  audit_error_logging(pii_component_id, Script_GUID, 'Failed', msg)
  
# Another call to garbage collection after the try-except block
gc.collect()

# COMMAND ----------

print(replace_list)

# COMMAND ----------

# DBTITLE 1,pii scrubbing - transcript customer
try:
  print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': starting presidio data anonymization for Script_Customer')

  replace_list = []
  print('Script_Customer: ', Script_Customer)
  Script_Customer_PIIScrubbed, Script_Customer_Analyzer_Results = analyzer_anonymizer(Script_Customer)
  print('Script_Customer_PIIScrubbed: ', Script_Customer_PIIScrubbed)

  # Invoke garbage collection
  gc.collect()

  print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': completed presidio data anonymization for Script_Customer')
  nb_status["script_customer_piiscrubbed"] = Script_Customer_PIIScrubbed
  nb_status["is_customer_piiscrubbed"] = True
  nb_status["script_piiscrubbeddate"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  nb_status["script_customer_analyzer_results"] = Script_Customer_Analyzer_Results
  nb_status["script_customer_piiscrubbed_values"] = replace_list

except Exception as e:
  msg = "Unable to PII scrub Script_Customer transcripts: " + str(e)
  nb_status["is_customer_piiscrubbed"] = False
  audit_error_logging(pii_component_id, Script_GUID, 'Failed', msg)
  
# Another call to garbage collection after the try-except block
gc.collect()

# COMMAND ----------

# DBTITLE 1,all replaced keywords
print(replace_list)

# COMMAND ----------

print(nb_status)

# COMMAND ----------

# DBTITLE 1,modified
# spark_df = spark.createDataFrame([nb_status])
# display(spark_df)
# spark_df.createOrReplaceTempView("temp_piiscrubbed")

schema = StructType([
    StructField("script_guid", StringType(), True),
    StructField("script_agent_piiscrubbed", StringType(), True),
    StructField("script_customer_piiscrubbed", StringType(), True),
    StructField("is_agent_piiscrubbed", StringType(), True),
    StructField("is_customer_piiscrubbed", StringType(), True),
    StructField("is_unauthenticated", StringType(), True),
    StructField("script_createddate", StringType(), True),
    StructField("script_piiscrubbeddate", StringType(), True),
    StructField("is_qacompleted", StringType(), True),
    StructField("script_agent_analyzer_results", ArrayType(StringType()), True),
    StructField("script_agent_piiscrubbed_values", ArrayType(StringType()), True),
    StructField("script_customer_analyzer_results", ArrayType(StringType()), True),
    StructField("script_customer_piiscrubbed_values", ArrayType(StringType()), True)
    # Add more columns and their corresponding types as needed
])
 
# Create the DataFrame with the specified schema
spark_df = spark.createDataFrame([nb_status], schema)
display(spark_df)
 
spark_df.createOrReplaceTempView("temp_piiscrubbed")

# COMMAND ----------

# DBTITLE 1,pii scrubbed table load
try:
  tgt_count_df = spark.sql("""select * from {0}.{1}.{2} where script_guid = '{3}'""". format(piiscrubbed_catalog_name, piiscrubbed_schema_name, piiscrubbed_table_name, Script_GUID))
  print(tgt_count_df.count())

  if tgt_count_df.count() == 0:
    spark.sql("""insert into {0}.{1}.{2}
(Script_GUID, Script_Agent_PIIScrubbed, Script_Customer_PIIScrubbed, Is_Agent_PIIScrubbed, Is_Customer_PIIScrubbed, Is_Unauthenticated, Script_CreatedDate, Script_PIIScrubbedDate, Is_QACompleted) 
select
s.Script_GUID, s.Script_Agent_PIIScrubbed, s.Script_Customer_PIIScrubbed, s.Is_Agent_PIIScrubbed, s.Is_Customer_PIIScrubbed, s.Is_Unauthenticated, cast(s.Script_CreatedDate as timestamp), current_timestamp(), s.Is_QACompleted from temp_piiscrubbed s
""".format(piiscrubbed_catalog_name, piiscrubbed_schema_name, piiscrubbed_table_name))

  else:
    spark.sql("""update {0}.{1}.{2} set 
Script_Agent_PIIScrubbed = '{4}',
Script_Customer_PIIScrubbed = '{5}',
Is_Agent_PIIScrubbed = '{6}',
Is_Customer_PIIScrubbed = '{7}',
Is_Unauthenticated = '{8}',
Script_CreatedDate = cast('{9}' as timestamp),
Script_PIIScrubbedDate = current_timestamp(),
Is_QACompleted = '{10}'
where Script_GUID = '{3}'""".format(piiscrubbed_catalog_name, piiscrubbed_schema_name, piiscrubbed_table_name, Script_GUID, nb_status["script_agent_piiscrubbed"].replace("'", "\\'"), nb_status["script_customer_piiscrubbed"].replace("'", "\\'"), nb_status["is_agent_piiscrubbed"], nb_status["is_customer_piiscrubbed"], nb_status["is_unauthenticated"], nb_status["script_createddate"], nb_status["is_qacompleted"]))
    
except Exception as e:
  msg = "Data insert to " + piiscrubbed_table_name + " got failed: " + str(e)
  audit_error_logging(pii_component_id, Script_GUID, 'Failed', msg)

# COMMAND ----------

# DBTITLE 1,pii scrubbed entity details table load
try:
  tgt_count_df = spark.sql("""select * from {0}.{1}.{2} where script_guid = '{3}'""". format(config_catalog_name, config_schema_name, piiscrubbed_entity_details_table_name, Script_GUID))
  print(tgt_count_df.count())

  if tgt_count_df.count() == 0:
    spark.sql("""insert into {0}.{1}.{2}
(account_name, Script_GUID, script_agent_analyzer_results, script_agent_piiscrubbed_values, script_customer_analyzer_results, script_customer_piiscrubbed_values, Script_PIIScrubbedDate) 
select
'{3}' as account_name, s.Script_GUID, s.script_agent_analyzer_results, s.script_agent_piiscrubbed_values, s.script_customer_analyzer_results, s.script_customer_piiscrubbed_values, current_timestamp() from temp_piiscrubbed s
""".format(config_catalog_name, config_schema_name, piiscrubbed_entity_details_table_name, account_name))

  else:
    spark.sql("""update {0}.{1}.{2} set 
account_name = '{3}',
script_agent_analyzer_results = '{4}',
script_agent_piiscrubbed_values = '{5}',
script_customer_analyzer_results = '{6}',
script_customer_piiscrubbed_values = '{7}',
Script_PIIScrubbedDate = current_timestamp()
where Script_GUID = '{8}'""".format(config_catalog_name, config_schema_name, piiscrubbed_entity_details_table_name, account_name, str(nb_status["script_agent_analyzer_results"]).replace("'", "\\'"), str(nb_status["script_agent_piiscrubbed_values"]).replace("'", "\\'"), str(nb_status["script_customer_analyzer_results"]).replace("'", "\\'"), str(nb_status["script_customer_piiscrubbed_values"]).replace("'", "\\'"), Script_GUID))

except Exception as e:
  msg = "Data insert to " + piiscrubbed_entity_details_table_name + " got failed: " + str(e)
  audit_error_logging(pii_component_id, Script_GUID, 'Failed', msg)

# COMMAND ----------

# DBTITLE 1,updating piiscrubbed flag in scripts table
try:
  spark.sql("""update {0}.{1}.{2} 
set PII_Scrubbed = 'Succeeded' where Script_GUID = '{3}';
""".format(scripts_catalog_name, scripts_schema_name, scripts_table_name, Script_GUID))
  msg = "pii scrubbed completed"
  audit_error_logging(pii_component_id, Script_GUID, 'Succeeded', msg)
  print(msg)

except Exception as e:
  msg = "piiscrubbed flag update in " + scripts_table_name + " got failed: " + str(e)
  print(msg)
  audit_error_logging(pii_component_id, Script_GUID, 'Failed', msg)

# COMMAND ----------

# DBTITLE 1,exiting notebook with output
dbutils.notebook.exit(json.dumps(nb_status))
