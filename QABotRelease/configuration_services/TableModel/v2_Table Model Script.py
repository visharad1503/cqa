# Databricks notebook source
catalog_name="genai_configuration"
schema_name="genai_audit_config"
# output_table_name="accounts"

# spark.sql("CREATE CATALOG IF NOT EXISTS {}".format(catalog_name))
# spark.sql("Use catalog {}".format(catalog_name))
# spark.sql("CREATE Schema IF NOT EXISTS {}".format(schema_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from genai_configuration.genai_audit_config.accounts

# COMMAND ----------

#create output table if not exists
# spark.sql("""
# create table if not exists {0}.{1}.{2}(
# account_Id string,
# accountName string,
# record_createddate timestamp,
# record_updateddated timestamp,
# is_enabled boolean)
# using delta
# location 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}'
# with (credential `difscaedifdunitycatalog`)
# """.format(catalog_name, schema_name, output_table_name))

# COMMAND ----------

output_table_name="Evaluationtag"

# COMMAND ----------

# DBTITLE 1,Evaluation_Tag
spark.sql("""
create table if not exists {0}.{1}.{2}(
EvaluationTag_Id int,
Evaluation_tag  string,
PK_Evaluationtag string)
using delta
location 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}'
with (credential `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))


# COMMAND ----------

output_table_name="prompts"

# COMMAND ----------

# DBTITLE 1,prompts
spark.sql("""
CREATE TABLE if not exists {0}.{1}.{2}(
    Prompts_Id INT NOT NULL,
    Account_Id STRING NOT NULL,
    Prompts STRING NOT NULL,
    IsEnable BOOLEAN,
    CONSTRAINT PK_Prompts PRIMARY KEY (Prompts_Id)
) USING DELTA
location 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}'
with (credential `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))

# ALTER TABLE genai_configuration.genai_audit_config.prompts 
# ADD CONSTRAINT FK_Prompts_accounts FOREIGN KEY (Account_Id) 
# REFERENCES genai_configuration.genai_audit_config.accounts (Account_Id);

# COMMAND ----------

output_table_name="promptsresults"
spark.sql("""
CREATE TABLE IF NOT EXISTS {0}.{1}.{2}(
    Prompts_Result_Id INT NOT NULL,
    Account_Id STRING NOT NULL,
    Prompts_Results STRING NOT NULL,
    Prompts_Id INT NOT NULL,
    CONSTRAINT PK_PromptsResults PRIMARY KEY (Prompts_Result_Id)
) USING DELTA
LOCATION 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/promptsresults'
WITH (CREDENTIAL `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))

# COMMAND ----------

output_table_name="account_qualityaudit_result"

spark.sql("""
CREATE TABLE IF NOT EXISTS {0}.{1}.{2}(
    Account_QualityAudit_Result_Id INT NOT NULL,
    Prompt_Id INT NOT NULL,
    Account_Id STRING NOT NULL,
    Result_Summary STRING NOT NULL,
    Result STRING NOT NULL,
    evaluation_tag_id INT NOT NULL,
    CONSTRAINT PK_Account_QualityAudit_Result PRIMARY KEY (Account_QualityAudit_Result_Id)
) USING DELTA
LOCATION 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/account_qualityaudit_result'
WITH (CREDENTIAL `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))

# COMMAND ----------

output_table_name="evaluationtag"
spark.sql("""
CREATE TABLE IF NOT EXISTS {0}.{1}.{2}(
    EvaluationTag_Id INT NOT NULL,
    Evaluation_tag STRING NOT NULL,
    CONSTRAINT PK_Evaluationtag PRIMARY KEY (EvaluationTag_Id)
) USING DELTA
LOCATION 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/evaluationtag'
WITH (CREDENTIAL `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))

# COMMAND ----------

spark.sql("""ALTER TABLE genai_configuration.genai_audit_config.auditscenarios 
ADD CONSTRAINT accounts_auditscenarios_fk 
FOREIGN KEY (account_Id) 
REFERENCES accounts(account_Id) """)
