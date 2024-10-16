# Databricks notebook source
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

dbutils.widgets.text("account_name", "")
account_names = dbutils.widgets.get("account_name").split(',')
workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')
print(workspaceUrl)
env='cqa'

# COMMAND ----------

# Function to run the query and call the result_presentation notebook
def process_account(account_name):
    component_name = 'Result Presentation'
    query = f"""
      SELECT account_id
      FROM {env}_genai_configuration.genai_audit_config.accounts
      WHERE lower(accountName) = lower('{account_name}')
      """
    result = spark.sql(query).collect()
    if not result:
        raise ValueError(f"No account found for account name: {account_name}")
    account_id = result[0]['account_id']

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
    result = df.collect()
    if not result:
        raise ValueError(f"No component found for account id: {account_id}")
    result = result[0]
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

    result_presentation_result = dbutils.notebook.run("./results_presentation_child", timeout_seconds, params)
    return result_presentation_result

# Use ThreadPoolExecutor to run the process_account function in parallel
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_account, account_name) for account_name in account_names]
    for future in as_completed(futures):
        result = future.result()
        print(result)

# COMMAND ----------


