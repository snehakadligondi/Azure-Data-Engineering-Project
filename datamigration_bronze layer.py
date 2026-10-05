# Databricks notebook source
# DBTITLE 1,load configuration settings
# MAGIC
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/configaration

# COMMAND ----------

# DBTITLE 1,import helper notebook functions
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/helper_notebook

# COMMAND ----------

# DBTITLE 1,initialize pipeline widget parameters

dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("notebook_name", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source", "")
dbutils.widgets.text("input_date", "")


pipeline_name = dbutils.widgets.get("pipeline_name")
notebook_name = dbutils.widgets.get("notebook_name")
run_id = dbutils.widgets.get("run_id")
source = dbutils.widgets.get("source")
input_date = dbutils.widgets.get("input_date")

# COMMAND ----------

# DBTITLE 1,date and previous month name extraction
parsed_date = datetime.strptime(input_date, "%Y%m%d")
prev_month_date = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
print(parsed_date)
print(prev_month_date)
print(prev_month_full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from external storage ADLS

# COMMAND ----------

raw_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/rawdata/"

# COMMAND ----------

# DBTITLE 1,load csv data and extract file path information
raw_df = spark.read.option("header", True).csv(raw_path).withColumn("file_path", col("_metadata.file_path"))
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding extra columns

# COMMAND ----------

# DBTITLE 1,extract city month and year from file path

new_df = raw_df.withColumn(
    "file_name_last", split("file_path", "/")[4]
).withColumn(
    "City", split(col("file_name_last"),'_')[0]
).withColumn(
    "Month", split(col("file_name_last"),'_')[1]
).withColumn(
    "Year", regexp_replace(split(col("file_name_last"),'_')[2], r"\.csv", "")
)
display(new_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### data quality check col validation 

# COMMAND ----------

validate_category_column(new_df)

# COMMAND ----------

check_string_length(new_df, 'Category', 100) 
#calling length check function from helper notebook
#check_string_length(raw_df, 'Category', 100) 
#calling length check function from helper notebook
check_string_length(new_df, 'BranchArea', 100)

# COMMAND ----------

validate_city_branchid(new_df) # to cehck chennai branch is getting only chennia records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null check on selected column
# MAGIC

# COMMAND ----------

null_checkist=['BranchId',
 'BranchArea',
 'productcode',
 'Category',
 'Category_Code',
 'Quantity',
 'salesdate',
 ]

# COMMAND ----------


null_counts_row = (
    new_df
    .select([col(c).isNull().alias(c) for c in null_checkist])
    .agg(*[sum(col(c).cast("int")).alias(c) for c in null_checkist])
    .collect()[0]
)
null_counts = {c: null_counts_row[c] for c in null_checkist}
display(null_counts)
if any(null_counts[c] > 0 for c in null_checkist):
    dbutils.notebook.exit("Null is there")

# COMMAND ----------

# DBTITLE 1,practice
check_duplicate_invoiceid(new_df)

# COMMAND ----------

# DBTITLE 1,practice
validate_quantity(new_df)

# COMMAND ----------

# Example usage:
validate_last_month_data(new_df,prev_month_full_name)  #checking the month

# COMMAND ----------

# DBTITLE 1,record audit entry for last month's data update
from datetime import date

write_audit_record(new_df, "bronze",prev_month_full_name)

# COMMAND ----------

# DBTITLE 1,fetch audit records for bronze notebook
# MAGIC %sql
# MAGIC select * from J2D_Audit_table where notebook_name='bronze_layer'

# COMMAND ----------

# DBTITLE 1,save processed data to parquet in bronze layer storage
new_df.write.mode("overwrite").parquet("abfss://datamigration@firstadlsstorage.dfs.core.windows.net/bronzelayerdata/")

# COMMAND ----------

success='All validations are successfully passed'
dbutils.notebook.exit(success)
