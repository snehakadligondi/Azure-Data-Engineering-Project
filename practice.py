# Databricks notebook source
# MAGIC
# MAGIC
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/configaration

# COMMAND ----------

# DBTITLE 1,Reading csv file format data
#dynamically reading the csv file 
def read_csv_file(file_path):
    #return spark.read.option("header", True).csv(file_path).withColumn("file_path", input_file_name())
    df=spark.read.option("header", True).csv(file_path)
    return df

# COMMAND ----------

raw_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/rawdata/"

# COMMAND ----------

raw_df=read_csv_file(raw_path)
display(raw_df)

# COMMAND ----------

# DBTITLE 1,reading json file format data
# dynamically reading the json file 
def read_json_file(file_path):
    df = spark.read.option("multiline", True).json(file_path)
    return df

# COMMAND ----------

raw_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/practice/"

# COMMAND ----------

raw_df=read_json_file(raw_path)
display(raw_df)

# COMMAND ----------

# 1. Read JSON file
def read_json_file(file_path):
    df = spark.read.option("multiline", True).json(file_path)
    return df

raw_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/practice/"

raw_df = read_json_file(raw_path)

# Display JSON data
#display(raw_df)


# 2. Create a new path for the recreated Parquet
parquet_output_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/practice/recreated_parquet/"


# 3. Write JSON DataFrame as a valid Parquet dataset
raw_df.write.mode("overwrite").parquet(parquet_output_path)


# 4. Function to read Parquet
def read_parquet_file(file_path):
    df = spark.read.parquet(file_path)
    return df


# 5. Read the newly created Parquet
parquet_df = read_parquet_file(parquet_output_path)


# 6. Display Parquet data
display(parquet_df)

# COMMAND ----------

excel_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/practice"

excel_file = [f.path for f in dbutils.fs.ls(excel_path) if f.path.endswith(".xlsx")][0]

excel_df = spark.read.format("excel").load(excel_file)

display(excel_df)