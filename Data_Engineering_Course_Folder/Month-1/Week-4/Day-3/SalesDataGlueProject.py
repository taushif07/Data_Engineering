from pyspark.context import SparkContext
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

# Create GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

sales_schema = StructType(
    [
        StructField("Product_ID", IntegerType(), True),
        StructField("Customer_ID", StringType(), True),
        StructField("Order_date", DateType(), True),
        StructField("Location", StringType(), True),
        StructField("Source_order", StringType(), True),
    ]
)

menu_schema = StructType(
    [
        StructField("Product_ID", IntegerType(), True),
        StructField("Product_name", StringType(), True),
        StructField("Product_price", StringType(), True),
    ]
)

sales_df = glueContext.read.csv(
    "/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-4/Day-3/sales.csv.txt",
    inferSchema=True,
    schema=sales_schema,
)
menu_df = glueContext.read.csv(
    "/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-4/Day-3/menu.csv.txt",
    inferSchema=True,
    schema=menu_schema,
)

sales_df.show(10, truncate=False)
menu_df.show(10, truncate=False)

sales_dynamic_DF = DynamicFrame.fromDF(sales_df, glueContext, "dynamic_df")
menu_dynamic_DF = DynamicFrame.fromDF(menu_df, glueContext, "dynamic_df")

# Question 1: Total amount spent by each customer

Total_Amount_Spent_Per_Customer = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Customer_ID"))
    .agg(func.sum("Product_price").alias("Total_Amount_Spent"))
    .orderBy(func.desc("Total_Amount_Spent"))
)

Total_Amount_Spent_Per_Customer.show()

# Question 2: Total amount spent on each food category

Total_Amount_Spent_Per_Food_Category = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Product_name"))
    .agg(func.sum("Product_price").alias("Total_Amount_Spent"))
    .orderBy(func.desc("Total_Amount_Spent"))
)

Total_Amount_Spent_Per_Food_Category.show()

# Question 3: Total Amount of sales in each month

Total_Amount_Of_Sales_Each_Month = (
    sales_df.withColumn("Month", func.month(func.col("Order_date")))
    .join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Month").alias("Order_Month"))
    .agg(func.sum("Product_price").alias("Total_Amount_Spent"))
    .orderBy(func.asc("Order_Month"))
)

Total_Amount_Of_Sales_Each_Month.show()
