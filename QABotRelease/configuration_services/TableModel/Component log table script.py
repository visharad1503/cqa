# Databricks notebook source
catalog_name = "dev_genai_configuration"
schema_name = "genai_audit_config"
log_table_name = "log"
component_table_name = "component"

# COMMAND ----------

spark.sql("""
CREATE TABLE if not exists {0}.{1}.{2} (
component_id int not null,
component_name string,
component_desc string,
created_date TIMESTAMP,
updated_date TIMESTAMP,
CONSTRAINT `component_pk` PRIMARY KEY (`component_id`))
USING delta
LOCATION 'abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/{2}'
with (credential `difscaedifdunitycatalog`)
""".format(catalog_name, schema_name, component_table_name))

# COMMAND ----------

spark.sql("""insert into {0}.{1}.{2} values (1, 'Audio', '', current_timestamp(), current_timestamp());""".format(catalog_name, schema_name, component_table_name))
spark.sql("""insert into {0}.{1}.{2} values (2, 'Transcript', '', current_timestamp(), current_timestamp());""".format(catalog_name, schema_name, component_table_name))
spark.sql("""insert into {0}.{1}.{2} values (3, 'PII transcript', '', current_timestamp(), current_timestamp());""".format(catalog_name, schema_name, component_table_name))
spark.sql("""insert into {0}.{1}.{2} values (4, 'Archive', '', current_timestamp(), current_timestamp());""".format(catalog_name, schema_name, component_table_name))

# COMMAND ----------

spark.sql("""insert into {0}.{1}.{2} values (5, 'Prompt ADLS file archive', '', current_timestamp(), current_timestamp(), '');""".format(catalog_name, schema_name, component_table_name))
spark.sql("""insert into {0}.{1}.{2} values (6, 'Prompt table insert', '', current_timestamp(), current_timestamp(), '');""".format(catalog_name, schema_name, component_table_name))

# COMMAND ----------

spark.sql("""insert into {0}.{1}.{2} values (7, 'Promptsresults table insert', '', current_timestamp(), current_timestamp(), '');""".format(catalog_name, schema_name, component_table_name))

# COMMAND ----------

spark.sql("""
CREATE TABLE if not exists {0}.{1}.{2} (
log_id string,
account_id int NOT NULL,
component_id int not null,
`guid` string,
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

dbutils.fs.ls('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/')

# COMMAND ----------

dbutils.fs.ls('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/')
#dbutils.fs.ls('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/log/')
#dbutils.fs.rm('abfss://genai-audit-config@difdusedls01sdl.dfs.core.windows.net/config/log/', True)

# COMMAND ----------

spark.sql("""
Drop TABLE if exists {0}.{1}.{2}
""".format(catalog_name, schema_name, log_table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from dev_genai_configuration.genai_audit_config.accounts
# MAGIC select distinct account_id from dev_genai_configuration.genai_audit_config.accounts where lower(accountname)='usbank'
