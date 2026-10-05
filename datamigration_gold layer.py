# Databricks notebook source
# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/configaration

# COMMAND ----------

# MAGIC %run /Workspace/Users/snehakadligondi@gmail.com/helper_notebook

# COMMAND ----------


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

parsed_date = datetime.strptime(input_date, "%Y%m%d")
prev_month_date = parsed_date - relativedelta(months=1)
prev_month_full_name = prev_month_date.strftime("%B")
print(parsed_date)
print(prev_month_date)
print(prev_month_full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total revenue by catagory

# COMMAND ----------

spark.sql("DELETE FROM gold.agg_category_sales")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.agg_category_sales
SELECT 
    p.category_code,
    p.category,
    SUM(s.revenue) AS total_revenue,
    SUM(s.quantity) AS total_quantity,
    'August' AS load_month,
    current_timestamp() AS created_at
FROM silver.dim_sales s
LEFT JOIN silver.dim_product p 
       ON s.product_code = p.product_code AND s.category_code = p.category_code
GROUP BY p.category_code, p.category
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_category_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Branch wise revenue

# COMMAND ----------

spark.sql("DELETE FROM gold.agg_branch_revenue")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.agg_branch_revenue 
SELECT 
    s1.branch_id,
    s2.branch_area,
    SUM(s1.revenue) AS total_revenue,
    SUM(s1.quantity) AS total_quantity,
    'August' AS load_month,
    current_timestamp() AS created_at
FROM silver.dim_sales s1
LEFT JOIN silver.dim_branch s2
       ON s1.branch_id = s2.branch_id 
GROUP BY s1.branch_id, s2.branch_area
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_branch_revenue 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Day wise sales

# COMMAND ----------

spark.sql("DELETE FROM gold.agg_daily_sales_by_dayname")

# COMMAND ----------

spark.sql("""
INSERT INTO gold.agg_daily_sales_by_dayname
SELECT  
    invoice_dt AS sales_date,
    date_format(invoice_dt,'EEEE') AS day_name,
    SUM(s.revenue) AS total_revenue,
    SUM(s.quantity) AS total_quantity,
    'August' AS load_month,
    current_timestamp() AS created_at
FROM silver.dim_sales s
GROUP BY invoice_dt, date_format(invoice_dt,'EEEE')
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  gold.agg_daily_sales_by_dayname

# COMMAND ----------

# MAGIC %md
# MAGIC ### most prefered TV's by revenue and quantity

# COMMAND ----------

spark.sql("DELETE FROM gold.agg_tv_product_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.agg_tv_product_sales
# MAGIC SELECT 
# MAGIC     p.category, 
# MAGIC     p.product_code,
# MAGIC     SUM(s.quantity) AS total_quantity,
# MAGIC     SUM(s.revenue) AS total_revenue,
# MAGIC     'August' AS load_month,
# MAGIC     current_timestamp() AS created_at
# MAGIC FROM silver.dim_sales s
# MAGIC LEFT JOIN silver.dim_product p 
# MAGIC        ON s.product_code = p.product_code AND s.Category_Code = p.category_code
# MAGIC WHERE p.product_code = 'TV'       -- filter only TV category
# MAGIC GROUP BY p.category, p.product_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_tv_product_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### which product going good in each branch

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold.agg_branch_product_revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.agg_branch_product_revenue
# MAGIC SELECT 
# MAGIC       s.branch_id,
# MAGIC       b.branch_area,
# MAGIC       s.product_code,
# MAGIC       p.category,
# MAGIC       SUM(s.revenue) AS total_revenue,
# MAGIC       SUM(s.quantity) AS total_quantity
# MAGIC   FROM silver.dim_sales s
# MAGIC   LEFT JOIN silver.dim_branch b 
# MAGIC          ON s.branch_id = b.branch_id
# MAGIC   LEFT JOIN silver.dim_product p 
# MAGIC          ON s.product_code = p.product_code AND s.category_code = p.category_code
# MAGIC   GROUP BY s.branch_id, b.branch_area, s.product_code, p.category

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.agg_branch_product_revenue

# COMMAND ----------

success='All reports has been generated successfully'
dbutils.notebook.exit(success)