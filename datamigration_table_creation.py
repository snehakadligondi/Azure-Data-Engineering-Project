# Databricks notebook source
# DBTITLE 1,Create Silver and Gold Databases if Not Exists
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver;
# MAGIC CREATE DATABASE IF NOT EXISTS gold;

# COMMAND ----------

# DBTITLE 1,Remove Existing Silver and Gold Tables for Cleanup
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.dim_branch;
# MAGIC DROP TABLE IF EXISTS silver.dim_product;
# MAGIC DROP TABLE IF EXISTS silver.dim_salesperson;
# MAGIC
# MAGIC DROP TABLE IF EXISTS silver.dim_payment;
# MAGIC DROP TABLE IF EXISTS silver.dim_delivery;
# MAGIC DROP TABLE IF EXISTS silver.dim_sales;
# MAGIC
# MAGIC DROP TABLE IF EXISTS gold.agg_category_sales;
# MAGIC DROP TABLE IF EXISTS gold.agg_daily_sales_by_dayname;
# MAGIC DROP TABLE IF EXISTS gold.agg_branch_revenue;
# MAGIC DROP TABLE IF EXISTS gold.agg_tv_product_sales;
# MAGIC DROP TABLE IF EXISTS gold.agg_branch_product_revenue;

# COMMAND ----------

# DBTITLE 1,Create Silver Dimension Table for Product Data
# MAGIC %sql
# MAGIC ---refere the price table to insert the data
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_product (
# MAGIC   product_code STRING,
# MAGIC   category STRING,
# MAGIC   category_code STRING,
# MAGIC   unit_price DOUBLE,
# MAGIC   effective_from DATE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_branch (
# MAGIC   branch_id STRING,
# MAGIC   branch_area STRING,
# MAGIC   manager_name STRING,
# MAGIC   effective_from DATE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_salesperson (
# MAGIC   salesperson_id STRING,
# MAGIC   salesperson_name STRING,
# MAGIC   branch_id STRING,
# MAGIC   effective_from DATE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_payment (
# MAGIC   payment_mode STRING,
# MAGIC   description STRING,
# MAGIC   effective_from DATE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_delivery (
# MAGIC   delivery_mode STRING,
# MAGIC   description STRING,
# MAGIC   effective_from DATE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_sales (
# MAGIC   invoice_id STRING,
# MAGIC   invoice_dt DATE,
# MAGIC   location STRING,
# MAGIC   branch_id STRING,
# MAGIC   product_code STRING,
# MAGIC   Category_Code STRING,
# MAGIC   salesperson_id STRING,
# MAGIC   payment_mode STRING,
# MAGIC   delivery_mode STRING,
# MAGIC   quantity INT,
# MAGIC   unit_price DOUBLE,
# MAGIC   revenue DOUBLE,
# MAGIC   load_ts TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.agg_category_sales (
# MAGIC   category_code STRING,
# MAGIC   category STRING,
# MAGIC   total_revenue DOUBLE,
# MAGIC   total_quantity BIGINT,
# MAGIC   load_month STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.agg_branch_revenue (
# MAGIC   branch_id STRING,
# MAGIC   branch_area STRING,
# MAGIC   total_revenue DOUBLE,
# MAGIC   total_quantity BIGINT,
# MAGIC   load_month STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.agg_daily_sales_by_dayname (
# MAGIC   sales_date DATE,
# MAGIC   day_name STRING,
# MAGIC   total_revenue DOUBLE,
# MAGIC   total_quantity BIGINT,
# MAGIC   load_month STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (sales_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.agg_tv_product_sales (
# MAGIC   category STRING,
# MAGIC   product_code STRING,
# MAGIC   total_quantity BIGINT,
# MAGIC   total_revenue DOUBLE,
# MAGIC   load_month STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.agg_branch_product_revenue (
# MAGIC   branch_id STRING,
# MAGIC   branch_area STRING,
# MAGIC   product_code STRING,
# MAGIC   category STRING,
# MAGIC   total_revenue DOUBLE,
# MAGIC   total_quantity BIGINT
# MAGIC ) USING DELTA;