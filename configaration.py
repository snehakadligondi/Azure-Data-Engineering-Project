# Databricks notebook source
# DBTITLE 1,Azure Storage Account Setup and Spark Configuration
# -------------------------
# CONFIGURATION
# -------------------------

# Replace with your values
storage_account_name = "firstadlsstorage"
container_name = "datamigration"
account_key = "nTNh+QYWlPAc6BRn2SwpD6L6rW69NuvfurLAlnfWlhgHUqApx/ROI0VO1UfPycejE/2Mcg90yxrZ+AStLROSQQ=="

# Secure option: Store the key in Databricks secrets instead of hardcoding
# account_key = dbutils.secrets.get(scope="my-scope", key="storage-account-key")

# -------------------------
# SETUP CONFIGURATION
# -------------------------
spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  account_key
)