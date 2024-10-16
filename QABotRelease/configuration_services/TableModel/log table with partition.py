# Databricks notebook source
catalog_name = "dev_genai_configuration"
schema_name = "genai_audit_config"
log_table_name = "log"
component_table_name = "component"

# COMMAND ----------

spark.sql("""
CREATE TABLE if not exists {0}.{1}.{2} (
log_id string,
account_id string NOT NULL,
component_id int not null,
`guid` string,
file_name string,
`status` string,
details string,
created_date TIMESTAMP,
updated_date TIMESTAMP
)
USING DELTA
OPTIONS (
  path 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}',
  credential 'difscaedifdunitycatalog'
)
PARTITIONED BY (`guid`)
""".format(catalog_name, schema_name, log_table_name))

# COMMAND ----------

spark.sql("""
Drop TABLE if exists {0}.{1}.{2}
""".format(catalog_name, schema_name, log_table_name))

# COMMAND ----------

dbutils.fs.ls('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/')
#dbutils.fs.rm('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/log', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table dev_genai_configuration.genai_audit_config.log
