# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE dev_genai_configuration.genai_audit_config.promptsresults (
# MAGIC   Prompts_Result_Id String,
# MAGIC   Account_Id STRING,
# MAGIC   Prompts_Id_Json STRING,
# MAGIC   Prompts_Results STRING,
# MAGIC   Script_GUID STRING,
# MAGIC   IsEnabled BOOLEAN,
# MAGIC   Is_Splitted BOOLEAN,
# MAGIC   Created_Date TIMESTAMP,
# MAGIC   Updated_Date TIMESTAMP,
# MAGIC   CreatedBy STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (Script_GUID)
# MAGIC LOCATION 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/promptsresults'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5')

# COMMAND ----------

#dbutils.fs.ls('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/promptsresults')
#dbutils.fs.rm('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/promptsresults', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from dev_genai_configuration.genai_audit_config.prompts
# MAGIC --select * from dev_genai_configuration.genai_audit_config.promptsresults
# MAGIC --select * from dev_genai_configuration.genai_audit_config.promptsresults_bkp
# MAGIC --show create table dev_genai_configuration.genai_audit_config.promptsresults
# MAGIC --create table dev_genai_configuration.genai_audit_config.promptsresults_bkp as select * from dev_genai_configuration.genai_audit_config.promptsresults
# MAGIC --drop table dev_genai_configuration.genai_audit_config.promptsresults
# MAGIC --show create table dev_genai_stg.usbank_transcriptor.account_qualityaudit_result
# MAGIC --drop table dev_genai_stg.usbank_transcriptor.account_qualityaudit_result
# MAGIC --create table dev_genai_stg.usbank_transcriptor.account_qualityaudit_result_bkp as select * from dev_genai_stg.usbank_transcriptor.account_qualityaudit_result
# MAGIC --show create table dev_genai_configuration.genai_audit_config.log
# MAGIC --select * from dev_genai_configuration.genai_audit_config.auditscenarios

# COMMAND ----------

#dbutils.fs.ls('abfss://dev-genai-stg@difdusedls01sdl.dfs.core.windows.net/usbank/')
#dbutils.fs.rm('abfss://dev-genai-stg@difdusedls01sdl.dfs.core.windows.net/usbank/account_qualityaudit_result', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists dev_genai_stg.usbank_transcriptor.account_qualityaudit_result (
# MAGIC   Account_QualityAudit_Result_Id String,
# MAGIC   Account_Id STRING,
# MAGIC   Script_GUID STRING,
# MAGIC   Audit_Scenario_id INT,
# MAGIC   Prompts_Id INT,  
# MAGIC   Verification_Type STRING,
# MAGIC   Evaluation_Tag_Id INT,
# MAGIC   Evaluation_Tag_Name STRING,
# MAGIC   Result_Status STRING,
# MAGIC   Result_Summary STRING,
# MAGIC   IsEnabled BOOLEAN,
# MAGIC   IsSplitComplteted BOOLEAN,
# MAGIC   Created_Date TIMESTAMP,
# MAGIC   Updated_Date TIMESTAMP,
# MAGIC   CreatedBy STRING)
# MAGIC USING DELTA
# MAGIC OPTIONS (
# MAGIC   path 'abfss://dev-genai-stg@difdusedls01sdl.dfs.core.windows.net/usbank/account_qualityaudit_result',
# MAGIC   credential 'difscaedifdunitycatalog'
# MAGIC )
# MAGIC PARTITIONED BY (Script_GUID)
