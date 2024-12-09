import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, sum, year, month
from botocore.exceptions import ClientError

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Source Database and S3 Target Configuration
source_database = "ecommerce-data-catalog"  # Replace with your Glue database name
output_s3_path = "s3://ecommerce-transformed-data/"  # Replace with your target bucket

# Load Tables into DataFrames
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_database, table_name="customer_details_csv"
).toDF()

ecommerce_sales_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_database, table_name="e_commerece_sales_data_2024_csv"
).toDF()

product_df = glueContext.create_dynamic_frame.from_catalog(
    database=source_database, table_name="product_details_csv"
).toDF()

# Join Tables Where Needed
customer_ecommerce_join = customer_df.join(
    ecommerce_sales_df, customer_df["customer id"] == ecommerce_sales_df["user id"], "inner"
)

product_ecommerce_join = product_df.join(
    ecommerce_sales_df, product_df["uniqe id"] == ecommerce_sales_df["product id"], "inner"
)

# Apply Transformations
results = {}

# Transformations on customer_details
results["customers_by_age"] = customer_df.groupBy("age").agg(count("*").alias("customer_count"))
results["customers_by_gender"] = customer_df.groupBy("gender").agg(count("*").alias("customer_count"))
results["purchase_sum_by_subscription_gender"] = customer_df.groupBy("subscription status", "gender") \
    .agg(sum("purchase amount (usd)").alias("total_purchase_amount"))

# Transformations requiring customer_details + e_commerece_sales_data_2024
results["customers_by_interaction_type"] = customer_ecommerce_join.groupBy("interaction type") \
    .agg(count("*").alias("interaction_count"))
results["customers_by_purchase_time"] = customer_ecommerce_join.groupBy("time stamp") \
    .agg(count("*").alias("purchase_count"))
results["product_purchase_frequency"] = customer_ecommerce_join.groupBy("item purchased") \
    .agg(count("purchase amount (usd)").alias("purchase_frequency"))

# Transformations on e_commerece_sales_data_2024
results["purchase_amount_by_year_month"] = customer_ecommerce_join.groupBy(
    year(col("time stamp")).alias("year"),
    month(col("time stamp")).alias("month")
).agg(sum("purchase amount (usd)").alias("total_purchase_amount"))

# Transformations on product_details
results["total_item_prices"] = product_df.agg(sum("selling price").alias("total_item_price"))
results["total_price_by_product_material"] = product_df.groupBy("product name", "category") \
    .agg(sum("selling price").alias("total_item_price"))

# Transformations requiring product_details + e_commerece_sales_data_2024
unique_product_count = product_ecommerce_join.select("product name").distinct().count()

# Write Results to S3
for key, result_df in results.items():
    output_path = f"{output_s3_path}{key}/"
    result_df.write.parquet(output_path, mode="overwrite")

job.commit()
