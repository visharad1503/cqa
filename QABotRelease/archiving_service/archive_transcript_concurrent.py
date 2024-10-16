# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

# DBTITLE 1,Import libraries
from azure.storage.blob import BlobServiceClient, ContainerClient
import uuid
import json
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,getting notebook parameters
dbutils.widgets.text("notebook_parameter","","") #getting values from parent Notebook through 
params = dbutils.widgets.get("notebook_parameter") #getting all values in strings
  
#Convert string values into dict for using it in locals() function (Unpacking purpose)
param = json.loads(params)
print(param)

#fetch all the values from dict without iteration (Can call any value through key)
locals().update(param)

print(param)

# COMMAND ----------

yearmonthdate = datetime.now().strftime('%Y%m%d')
print(yearmonthdate)

# COMMAND ----------

# DBTITLE 1,getting connection string
connection_string = dbutils.secrets.get(scope, key=connection_string_key)

if connection_string is None:
  print("Missing required key vault value: connection_string.")

# COMMAND ----------

# DBTITLE 1,variables
scriptarchive_container = script_container+'archive'

log_table_name = 'log'
accounts_table_name = 'accounts'
component_table_name = 'component'

nb_status={
  "guid": guid,
  "archival_copy_status": '',
  "details": ''
}

# COMMAND ----------

# DBTITLE 1,user defined functions
#logging
def audit_error_logging(guid, file_name, status, error):
  updated_error = str(error).replace('\'','').lower()
  try:
    spark.sql("""insert into {0}.{1}.{2} (log_id, account_id, component_id, guid, file_name, status, details, created_date, updated_date)
select uuid() as log_id,
'{3}' as  account_id,
{4} as component_id,
'{5}' as guid,
'{6}' as file_name,
'{7}' as status,
case when isnull('{8}') then null else '{8}' end as details,
current_timestamp(),
current_timestamp()""".format(config_catalog_name, config_schema_name, log_table_name, account_id, component_id, guid, file_name, status, updated_error))
    #print('log data inserted successfully')
  except Exception as e:
    print('error while inserting data into log table')
    print(e)

# COMMAND ----------

# DBTITLE 1,creating blob and container clients
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
source_container_client = ContainerClient.from_connection_string(connection_string, script_container)
archive_container_client = ContainerClient.from_connection_string(connection_string, scriptarchive_container)

# COMMAND ----------

# DBTITLE 1,source to archive blob copy
# Generate UUID and paths
d = uuid.UUID(hex=guid)
guid_directory_name = str(d)

# List all blobs in the source container
blob_list = [b.name for b in list(source_container_client.list_blobs())]
print("blob_list: ", blob_list)

for source_blob_path in blob_list:
    # print("source_blob_path: ", source_blob_path)
    
    # Extract directory name and file name from the source blob path
    directory_name = source_blob_path.split('/')[0]
    file_name = source_blob_path.split('/')[-1]
    
    destination_blob_path = f"{yearmonthdate}/{directory_name}/{file_name}"
    # print("destination_blob_path: ", destination_blob_path)
    
    source_blob_client = blob_service_client.get_blob_client(container=script_container, blob=source_blob_path)
    destination_blob_client = blob_service_client.get_blob_client(container=scriptarchive_container, blob=destination_blob_path)
    
    if guid_directory_name in source_blob_path:
        print("source_blob_path: ", source_blob_path)
        print('guid blob path exists in source container')
        print("destination_blob_path: ", destination_blob_path)
        try:
            copy_blob_stats = destination_blob_client.start_copy_from_url(source_blob_client.url)
            print(copy_blob_stats.get('copy_status'))
            if copy_blob_stats.get('copy_status') == 'success':
                try:
                    source_container_client.delete_blob(source_blob_path, delete_snapshots='include')
                    print('Source to archive blob copy and source blob deletion are successful')
                    audit_error_logging(directory_name, file_name, 'Succeeded', 'Source to archive blob copy and source blob deletion are successful')
                    nb_status["archival_copy_status"] = 'Succeeded'
                    nb_status["details"] = 'Source to archive blob copy and source blob deletion are successful'
                except Exception as e:
                    print('Source to archive blob copy is successful but source blob deletion got failed')
                    audit_error_logging(directory_name, file_name, 'Failed', 'Source to archive blob copy is successful but source blob deletion got failed. Please check the databricks log for more details')
                    nb_status["archival_copy_status"] = 'Failed'
                    nb_status["details"] = 'Source to archive blob copy is successful but source blob deletion got failed'
            else:
                audit_error_logging(directory_name, file_name, 'Failed', 'Source to archive blob copy is not successful. Please check the databricks log for more details')
                nb_status["archival_copy_status"] = 'Failed'
                nb_status["details"] = 'Source to archive blob copy is not successful'
        except Exception as e:
            audit_error_logging(directory_name, file_name, 'Failed', 'Source to archive blob copy got failed. Please check the databricks log for more details')
            nb_status["archival_copy_status"] = 'Failed'
            nb_status["details"] = 'Source to archive blob copy got failed'
    # else:
    #     print('blob path does not exist in source container')
    #     nb_status["archival_copy_status"] = 'NA'
    #     nb_status["details"] = 'blob path does not exist in source container'
    #     audit_error_logging(directory_name, file_name, 'NA', 'blob path does not exist in source container')

# COMMAND ----------

# DBTITLE 1,exiting concurrent notebook
dbutils.notebook.exit(json.dumps(nb_status))
