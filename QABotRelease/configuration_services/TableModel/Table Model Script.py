# Databricks notebook source
catalog_name="genai_configuration"
schema_name="genai_audit_config"
# output_table_name="accounts"

spark.sql("CREATE CATALOG IF NOT EXISTS {}".format(catalog_name))
spark.sql("Use catalog {}".format(catalog_name))
spark.sql("CREATE Schema IF NOT EXISTS {}".format(schema_name))

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

output_table_name="auditScenarios"

# COMMAND ----------

spark.sql("""
create table if not exists {0}.{1}.{2}(
audit_Scenario_id int,
account_Id  string,
category string,
source string,
Criticality string,
evaluation_tag string,
question string,
Criteria string,
Explanation string,
Example  string,
updated_date timestamp,
Created_date timestamp,
Created_By string,
is_enabled boolean)
using delta
location 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}'
with (credential `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, output_table_name))


# COMMAND ----------



spark.sql("""ALTER TABLE genai_configuration.genai_audit_config.auditscenarios 
ADD CONSTRAINT accounts_auditscenarios_fk 
FOREIGN KEY (account_Id) 
REFERENCES accounts(account_Id) """)
