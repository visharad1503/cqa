# Databricks notebook source
import json

# COMMAND ----------

dbutils.widgets.text("account_name", "cqa")
account_name = dbutils.widgets.get("account_name")
workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')
print(workspaceUrl)
env='cqa'

# COMMAND ----------

query = f"""
SELECT account_id
FROM {env}_genai_configuration.genai_audit_config.accounts
WHERE lower(accountName) = lower('{account_name}')
"""

result = spark.sql(query).collect()
account_id=result[0]['account_id']


# COMMAND ----------

# DBTITLE 1,Transcript
# Run the query and load the result into a DataFrame
component_name='Transcript'
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


# Extract the values into variables
result = df.collect()[0]
component_id = result['component_id']
timeout_seconds = result['timeout_seconds']
parameter_type = result['parameter_type']
num_workers = result['num_workers']

params = {
    "account_id": account_id,
    "account_name": account_name,
    "env": env,
    "component_id": component_id,
    "timeout_seconds": timeout_seconds,
    "num_workers": num_workers,

}
print(params)


genai_transcriptor_result = dbutils.notebook.run("./json_extractor_service/main_genai_transcriptor_service",timeout_seconds , params)

# COMMAND ----------

genai_transcriptor_result = "Success"

# COMMAND ----------

# DBTITLE 1,Audio Archive
if genai_transcriptor_result == "Success":
  component_name='Audio Archive'
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


  # Extract the values into variables
  result = df.collect()[0]
  component_id = result['component_id']
  timeout_seconds = result['timeout_seconds']
  parameter_type = result['parameter_type']
  num_workers = result['num_workers']

  params = {
      "account_id": account_id,
      "account_name": account_name,
      "env": env,
      "component_id": component_id,
      "timeout_seconds": timeout_seconds,
      "num_workers": num_workers,

  }
  print(params)
  audio_archive_transcript_result = dbutils.notebook.run("./archiving_service/audio_archive_transcript",timeout_seconds , params)
else:
    print("genai_transcriptor_v2 execution failed")

# COMMAND ----------

# %pip install  langchain 
# %pip install langchain-openai 
# %pip install langchain-experimental 
# !pip install presidio-analyzer
# !pip install presidio-anonymizer
# !pip install spacy Faker

# COMMAND ----------

audio_archive_transcript_result = "Success"

# COMMAND ----------

# DBTITLE 1,PII transcript
if audio_archive_transcript_result == "Success":
  component_name='PII transcript'
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


  # Extract the values into variables
  result = df.collect()[0]
  component_id = result['component_id']
  timeout_seconds = result['timeout_seconds']
  parameter_type = result['parameter_type']
  num_workers = result['num_workers']

  # params = {
  #     "account_id": account_id,
  #     "account_name": account_name,
  #     "env": env,
  #     "component_id": component_id,
  #     "timeout_seconds": timeout_seconds,
  #     "num_workers": num_workers,

  # }
  params = {
      "account_id": account_id,
      "account_name": account_name,
      "env": env,
      "component_id": component_id,
      "timeout_seconds": timeout_seconds,
      "num_workers": num_workers,

  }
  print(params)
  piiscrub_transcript_result = dbutils.notebook.run("./pii_scrub_service/main_piiscrub_transcript_service",timeout_seconds , params)
else:
    print("genai_transcriptor_v2 execution failed")

# COMMAND ----------

piiscrub_transcript_result = "Success"

# COMMAND ----------

# DBTITLE 1,Archive
if piiscrub_transcript_result == "Success":
  component_name='Archive'
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


  # Extract the values into variables
  result = df.collect()[0]
  component_id = result['component_id']
  timeout_seconds = result['timeout_seconds']
  parameter_type = result['parameter_type']
  num_workers = result['num_workers']

  params = {
      "account_id": account_id,
      "account_name": account_name,
      "env": env,
      "component_id": component_id,
      "timeout_seconds": timeout_seconds,
      "num_workers": num_workers,

  }
  print(params)
  archive_transcript_result = dbutils.notebook.run("./archiving_service/archive_transcript",timeout_seconds , params)
else:
    print("genai_transcriptor_v2 execution failed")

# COMMAND ----------

# DBTITLE 1,Promptsresults table insert
if archive_transcript_result == "Success":
  component_name='Promptsresults table insert'
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


  # Extract the values into variables
  result = df.collect()[0]
  component_id = result['component_id']
  timeout_seconds = result['timeout_seconds']
  parameter_type = result['parameter_type']
  num_workers = result['num_workers']

  params = {
      "account_id": account_id,
      "account_name": account_name,
      "env": env,
      "component_id": component_id,
      "timeout_seconds": timeout_seconds,
      "num_workers": num_workers,

  }
  print(params)
  piiscrub_transcript_result = dbutils.notebook.run("./prompt_execution_engine/main_prompt_execution",timeout_seconds , params)
else:
    print("archive_transcript_result execution failed")

# COMMAND ----------


