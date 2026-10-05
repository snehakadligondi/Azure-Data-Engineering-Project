# Databricks notebook source
# DBTITLE 1,importing datetime and spark sql functions
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import col, split, regexp_replace
from pyspark.sql.functions import col, instr, lit

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta, date

# COMMAND ----------

from pyspark.sql.functions import split, col, regexp_replace
from pyspark.sql.functions import col, sum
from pyspark.sql.functions import length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import lit, current_timestamp


# COMMAND ----------

# DBTITLE 1,dynamically loading CSV files with Spark and input path
#dynamically reading the csv file 
def read_csv_file(file_path):
    return spark.read.option("header", True).csv(file_path).withColumn("file_path", input_file_name())
    df=spark.read.option("header", True).csv(file_path)
    return df

# COMMAND ----------

# DBTITLE 1,validate category values in DataFrame for accuracy
def validate_category_column(df):
    valid_categories = ["TV", "FR", "WM", "AC"]
    invalid_count = df.filter(~col("Category").substr(lit(1), instr(col("Category"), "-") - 1).isin(valid_categories)).count()

    if invalid_count > 0:
        dbutils.notebook.exit("Invalid Category value found")

# COMMAND ----------

# DBTITLE 1,validate string length compliance in DataFrame column
def check_string_length(df, colname, max_length):
    if df.filter(length(colname) > max_length).count() > 0:
        dbutils.notebook.exit(f"String length check failed: Values in column '{colname}' exceed {max_length} characters.")

# COMMAND ----------

# DBTITLE 1,validate city branch ID format in DataFrame entries
def validate_city_branchid(df):
    from pyspark.sql.functions import col, substring
    invalid = df.filter(
        ~(
            ((col("City") == "Chennai") & (substring(col("BranchId"), 1, 3) == "CHN")) |
            ((col("City") == "Bangalore") & (substring(col("BranchId"), 1, 3) == "BLR"))
        )
    )
    if invalid.count() > 0:
        dbutils.notebook.exit("there is a issue in  data and we are getting irrelevant data")

# COMMAND ----------

# DBTITLE 1,practice
def check_duplicate_invoiceid(df):
    return df.groupBy("InvoiceId").count().filter("count > 1").count() > 0

# COMMAND ----------

# DBTITLE 1,practice
def validate_quantity(df):
    invalid_count = df.filter(col("Quantity") <= 0).count()

    if invalid_count > 0:
        dbutils.notebook.exit("Invalid Quantity found")

# COMMAND ----------

# DBTITLE 1,check for irrelevant data in last month's DataFrame
def validate_last_month_data(df,prev_month_full_name):
    
    if df.filter(col("Month") != prev_month_full_name).count() > 0:
        dbutils.notebook.exit("Irrelevant data")

# COMMAND ----------

# DBTITLE 1,create and save audit records in Spark DataFrame
audit_schema = StructType([
    StructField("pipeline_name", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("load_time", DateType(), True),
    StructField("status", StringType(), True),
    StructField("report_month", StringType(), True)
])

# from pyspark.sql.functions import lit, current_date

def write_audit_record(df, layer, report_month):
    audit_df = spark.createDataFrame(
        [(pipeline_name, notebook_name, run_id, source, layer, df.count(), date.today(), "Success", report_month)],
        schema=audit_schema
    )
    audit_df.write.mode("append").saveAsTable("J2D_Audit_table")