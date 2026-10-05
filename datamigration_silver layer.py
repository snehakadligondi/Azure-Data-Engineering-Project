# Databricks notebook source
# DBTITLE 1,Import User Configuration Settings for Environment Init ...
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/configaration

# COMMAND ----------

# DBTITLE 1,Load Helper Notebook for User Configuration Setup
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/helper_notebook

# COMMAND ----------

# DBTITLE 1,Initialize Pipeline Parameters for Notebook Execution

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

# DBTITLE 1,Parse and Calculate Previous Month from Input Date
parsed_date = datetime.strptime(input_date, "%Y%m%d")
prev_month_date = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
print(parsed_date)
print(prev_month_date)
print(prev_month_full_name)

# COMMAND ----------

Price_path = "abfss://datamigration@firstadlsstorage.dfs.core.windows.net/Referencefileforprice/"

# COMMAND ----------

df_prices = spark.read.option("header", "true").csv(Price_path)
display(df_prices)

# COMMAND ----------

# DBTITLE 1,Transform and Enrich Price DataFrame with Product Names ...

# we need to do some transformation on the  price dataframe
df_prices = df_prices.selectExpr(
    """
    CASE 
        WHEN product_code = 'p1' THEN 'TV'
        WHEN product_code = 'p2' THEN 'AC'
        WHEN product_code = 'p3' THEN 'Washing-Machine'
        WHEN product_code = 'p4' THEN 'Fridge'
        ELSE product_code
    END as product_code
    """,
    "split(category, '-')[1] as category",
    "category_code",
    "cast(unit_price as decimal(10,2)) as unit_price"
)

df_prices = (
    df_prices.withColumn("effective_from", lit(prev_month_date.date()))  #etl columns to show which month data
             .withColumn("load_ts", current_timestamp())  # it is showing load time 
)
df_prices.createOrReplaceTempView("tmp_prices")  # tem view created  just to insert the data into dim_product table 
display(df_prices)

# COMMAND ----------

# DBTITLE 1,Clear Existing Data from Silver Dim Product Table Befor ...
#before loading the data into this table we are going to delete the data
spark.sql("DELETE FROM silver.dim_product")

# COMMAND ----------

# DBTITLE 1,Insert New Records into Silver Dim Product Table
#inserting the new data into silver.dim_product
spark.sql("""
  INSERT INTO silver.dim_product
  SELECT * FROM tmp_prices
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_product

# COMMAND ----------

# DBTITLE 1,path to Read  bronze layer data (parquet)
bronze_data="abfss://datamigration@firstadlsstorage.dfs.core.windows.net/bronzelayerdata/"

# COMMAND ----------

# DBTITLE 1,Load and Display Bronze Layer Data from Parquet File
df_bronze = spark.read.parquet(bronze_data)
display(df_bronze)

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records Based on InvoiceId and Product ...
#dropDuplicates to drop the duplicates based on InvoiceId and productcode
#if it is coming two times it will keep only one record 
stg_df = df_bronze.dropDuplicates(["InvoiceId","productcode"])

# COMMAND ----------

stg_df.display()

# COMMAND ----------

from pyspark.sql.functions import when, col
#here replacing the null with case wehn statemnet 
stg_df = stg_df.withColumn(
    "PaymentMode",
    when(col("PaymentMode").isNull(), "Cash")
    .otherwise(col("PaymentMode"))
)

# COMMAND ----------

#here again replacing null with fillna
stg_df = stg_df.fillna({'delivery_mode': 'pickup'})

# COMMAND ----------

staging_deduped_rows = stg_df.count()
print("Staging deduped rows:", staging_deduped_rows)

# COMMAND ----------

display(stg_df)


# COMMAND ----------

# DBTITLE 1,unique branch data
#before loading the data into this table we are going to delete the data
spark.sql("DELETE FROM silver.dim_branch")

# COMMAND ----------

new_branches = stg_df.select("BranchId", "BranchArea", "manager_name") \
    .where(col("BranchId").isNotNull()) \
    .dropDuplicates(["BranchId", "BranchArea", "manager_name"]) \
    .withColumn("effective_from", lit(prev_month_date.date())) \
    .withColumn("load_ts", current_timestamp())

# Create a temp view
new_branches.createOrReplaceTempView("new_branches")

# Insert into the table
spark.sql("""
    INSERT INTO silver.dim_branch
    SELECT * FROM new_branches
""")
print("dim_branch - new keys inserted (if any).")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_branch

# COMMAND ----------

#before loading the data into this table we are going to delete the data
spark.sql("DELETE FROM silver.dim_salesperson")

# COMMAND ----------

new_salespersons = stg_df.select("SalespersonId", "BranchId") \
    .where(col("SalespersonId").isNotNull()) \
    .dropDuplicates(["SalespersonId", "BranchId"]) \
    .withColumn("salesperson_name", lit(None)) \
    .withColumn("branch_id", col("BranchId")) \
    .withColumn("effective_from", lit(prev_month_date.date())) \
    .withColumn("load_ts", current_timestamp()) \
    .select("SalespersonId", "salesperson_name", "branch_id", "effective_from", "load_ts")

new_salespersons.createOrReplaceTempView("new_salespersons")

spark.sql("""
    INSERT INTO silver.dim_salesperson
    SELECT * FROM new_salespersons
""")
print("dim_salesperson - new keys inserted (if any).")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_salesperson

# COMMAND ----------

#before loading the data into this table we are going to delete the data
spark.sql("DELETE FROM silver.dim_payment")

# COMMAND ----------

new_payments = stg_df.select("PaymentMode") \
    .where(col("PaymentMode").isNotNull()) \
    .dropDuplicates(["PaymentMode"]) \
    .withColumn("description", lit(None)) \
    .withColumn("effective_from", lit(prev_month_date.date())) \
    .withColumn("load_ts", current_timestamp()) \
    .select("PaymentMode", "description", "effective_from", "load_ts")

new_payments.createOrReplaceTempView("new_payments")

spark.sql("""
    INSERT INTO silver.dim_payment
    SELECT * FROM new_payments
""")
print("dim_payment - new keys inserted (if any).")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_payment

# COMMAND ----------

#before loading the data into this table we are going to delete the data
spark.sql("DELETE FROM silver.dim_delivery")

# COMMAND ----------

new_delivery= stg_df.select("delivery_mode") \
    .where(col("delivery_mode").isNotNull()) \
    .dropDuplicates(["delivery_mode"]) \
    .withColumn("description", lit(None)) \
    .withColumn("effective_from", lit(prev_month_date.date())) \
    .withColumn("load_ts", current_timestamp()) \
    .select("delivery_mode", "description", "effective_from", "load_ts")

new_delivery.createOrReplaceTempView("new_delivery")

spark.sql("""
    INSERT INTO silver.dim_delivery
    SELECT * FROM new_delivery
""")
print("dim_delivery - new keys inserted (if any).")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_delivery

# COMMAND ----------

from pyspark.sql.functions import when

stg_df = stg_df.withColumn(
    "productcode",
    when(stg_df.productcode == "p1", "TV")
    .when(stg_df.productcode == "p2", "AC")
    .when(stg_df.productcode == "p3", "Washing-Machine")
    .when(stg_df.productcode == "p4", "Fridge")
    .otherwise(stg_df.productcode)
)

# COMMAND ----------

stg_df.display()

# COMMAND ----------

spark.sql("DELETE FROM silver.dim_sales")

# COMMAND ----------

dim_product_df = spark.table("silver.dim_product").withColumnRenamed('category_code', 'category_code_dim')\
    .withColumnRenamed('product_code','product_code_dim')

# COMMAND ----------

from pyspark.sql.functions import to_date, substring, col, coalesce, current_timestamp

# Join stg_df with dim_product_df using PySpark

tmp_fact_df = stg_df.join(dim_product_df, (stg_df.productcode == dim_product_df.product_code_dim)
                            & (stg_df.Category_Code == dim_product_df.category_code_dim)
                           , "left")\
.withColumn(
    "invoice_dt", to_date(substring(col("InvoiceId"), 12, 8), "yyyyMMdd")
).withColumn(
    "unit_price", coalesce(col("unit_price"), lit(0).cast("double"))   #  coalesce: It takes the first non-null value
).withColumn(
    "revenue", col("Quantity").cast("int") * coalesce(col("unit_price"), lit(0).cast("double"))
).withColumn(
    "load_ts", current_timestamp()
).select(
    "InvoiceId",
    "invoice_dt",
    "BranchArea",
    "BranchId",
    col('product_code_dim').alias('productcode'),
    col('category_code_dim').alias('category_code'),
    "SalespersonId",
    "PaymentMode",
    "delivery_mode",
    "Quantity",
    "unit_price",
    "revenue",
    "load_ts"
)

tmp_fact_df.createOrReplaceTempView("tmp_fact")



display(tmp_fact_df)

# COMMAND ----------

spark.sql("""
    INSERT INTO silver.dim_sales
    SELECT * FROM tmp_fact
""")
print("Appended facts into silver.fact_sales ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.dim_sales

# COMMAND ----------

tmp_fact_count = tmp_fact_df.count()
print("Staging deduped rows:", tmp_fact_count)

# COMMAND ----------

def compare_and_exit(val1: int, val2: int):
    if val1 != val2:
        dbutils.notebook.exit(f"Values do not match: {val1} != {val2}")

# COMMAND ----------

compare_and_exit(staging_deduped_rows,tmp_fact_count)

# COMMAND ----------

success='All validations are successfully passed'
dbutils.notebook.exit(success)