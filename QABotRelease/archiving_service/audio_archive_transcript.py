# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import pandas as pd

# COMMAND ----------

dbutils.widgets.text("account_id","")
dbutils.widgets.text("account_name","")
dbutils.widgets.text("env","")
dbutils.widgets.text("component_id","")
dbutils.widgets.text("timeout_seconds","")
dbutils.widgets.text("num_workers","")

account_id=dbutils.widgets.get("account_id")
account_name=dbutils.widgets.get("account_name")
env=dbutils.widgets.get("env")
component_id=dbutils.widgets.get("component_id")
timeout_seconds=int(dbutils.widgets.get("timeout_seconds"))
num_workers=int(dbutils.widgets.get("num_workers"))

# Query the table to get the ParameterJson
query = f"""
SELECT ParameterJson
FROM {env}_genai_configuration.genai_audit_config.parameter
WHERE ComponentId = '{component_id}' AND AccountId = '{account_id}' and IsEnable='True'
"""

# Execute the query and get the result
parameter_json_df = spark.sql(query)
parameter_json_str = parameter_json_df.collect()[0]['ParameterJson']

# Parse the JSON string into a dictionary
parameters = json.loads(parameter_json_str)
print(parameters)

# COMMAND ----------

# account_name = dbutils.widgets.get("account_name")
storage_account = parameters["storage_account_name"]
catalog = parameters["catalog"]
scripts_schema_name = parameters["scripts_schema_name"]
scope = parameters["scope"]
source_audio_container_name = parameters["source_audio_container_name"]
archive_audio_container_name = parameters["archive_audio_container_name"]
connection_string_key=parameters["connection_string_key"]

catalog=f"{env}_{catalog}"

connection_string = dbutils.secrets.get(scope, key=connection_string_key)        
storage_account_url = 'https://'+storage_account+'.blob.core.windows.net'

if connection_string is None:
  print("Missing required key vault value: difqabotdwsconstr.")

# COMMAND ----------

monthyear = datetime.now().strftime('%m%Y')
scripts_table_name = 'scripts_' + monthyear

# COMMAND ----------

# DBTITLE 1,Extract Unique Audio IDs from Scripts in Spark
audio_files = spark.sql(f"""
SELECT collect_list(distinct split_part(Script_GUID, '_', 3)) as audio_id_list
FROM {catalog}.{scripts_schema_name}.{scripts_table_name}
WHERE isAudioArchived = 'False' AND Details = 'NA'
""").collect()

audio_id_list = audio_files[0]['audio_id_list']
print(audio_id_list)

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)

# Function to check if a container exists
def container_exists(container_client):
    try:
        container_client.get_container_properties()
        return True
    except:
        return False

# Get the container clients
source_container_client = blob_service_client.get_container_client(source_audio_container_name)
archive_container_client = blob_service_client.get_container_client(archive_audio_container_name)

# Check if the source and archive containers exist
if not container_exists(source_container_client):
    raise Exception(f"Source container '{source_audio_container_name}' does not exist.")
if not container_exists(archive_container_client):
    raise Exception(f"Archive container '{archive_audio_container_name}' does not exist.")

# List blobs in the source container
blobs_list = source_container_client.list_blobs()

for blob in blobs_list:
    # Check if the blob name contains any of the audio IDs
    for audio_id in audio_id_list:
        if audio_id in blob.name:
            source_blob = source_container_client.get_blob_client(blob)
            archive_blob = archive_container_client.get_blob_client(blob.name)
            
            # Copy the blob to the archive container
            copy_source = f"{storage_account_url}/{source_audio_container_name}/{blob.name}"
            print(copy_source)
            archive_blob.start_copy_from_url(copy_source)
            
            # Delete the original blob from the source container
            source_blob.delete_blob()
            
            # Print the name of the moved file
            print(f"Moved file: {blob.name}")
            
            # Update the isAudioArchived field in the SQL table
            spark.sql(f"""
            UPDATE {catalog}.{scripts_schema_name}.{scripts_table_name}
            SET isAudioArchived = 'True'
            WHERE split_part(Script_GUID, '_', 3) = '{audio_id}'
            """)
            break

print("Files moved to archive and isAudioArchived updated successfully.")

# COMMAND ----------

dbutils.notebook.exit("Success")
