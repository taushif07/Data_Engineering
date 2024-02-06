from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder.master("local[*]").appName("Sales_App").getOrCreate()

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

sales_df = spark.read.csv("./sales.csv.txt", inferSchema=True, schema=sales_schema)
menu_df = spark.read.csv("./menu.csv.txt", inferSchema=True, schema=menu_schema)

sales_df.show()
menu_df.show()

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

# Question 4: Total Amount of sales yearly

Total_Amount_Of_Sales_Yearly = (
    sales_df.withColumn("Year", func.year(func.col("Order_date")))
    .join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Year").alias("Order_Year"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Amount_Spent"))
    .orderBy(func.asc(func.col("Order_Year")))
)

Total_Amount_Of_Sales_Yearly.show()

# Question 5: Total Amount of Sales quaterly

Total_Amount_Of_Sales_Quaterly = (
    sales_df.withColumn("Quater", func.quarter(func.col("Order_date")))
    .join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Quater").alias("Order_Quater"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Amount_Spent"))
    .orderBy(func.asc(func.col("Order_Quater")))
)

Total_Amount_Of_Sales_Quaterly.show()

# Question 6: How many times each product has been purchased

No_Of_Times_Each_Product_Purchased = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .drop(menu_df["Product_ID"])
    .groupBy(func.col("Product_ID"), func.col("Product_name"))
    .agg(func.count(func.col("Product_ID")).alias("Total_Items_Purchased"))
    .orderBy(func.desc(func.col("Total_Items_Purchased")))
)

No_Of_Times_Each_Product_Purchased.show()

# Question 7: find the top 5 Order Items

Top_Five_Order = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .drop(menu_df["Product_ID"])
    .groupBy(func.col("Product_ID"), func.col("Product_name"))
    .agg(func.count(func.col("Product_ID")).alias("Total_Items_Purchased"))
    .orderBy(func.desc(func.col("Total_Items_Purchased")))
    .limit(5)
)

Top_Five_Order.show()


# Question 8: find the frequency of customer visited

Frequency_Of_Customer_Visited = (
    sales_df.groupBy(func.col("Customer_ID"))
    .agg(func.count(func.col("Customer_ID")).alias("Customer_Frequency"))
    .orderBy(func.desc(func.col("Customer_Frequency")))
)

Frequency_Of_Customer_Visited.show()


# Question 9: find the frequency of customer visited to the Restaurant

Frequency_Of_Customer_Visited_To_Restaurant = (
    sales_df.filter(func.col("Source_order") == "Restaurant")
    .groupBy(func.col("Customer_ID"))
    .agg(func.count(func.col("Customer_ID")).alias("Customer_Frequency"))
    .orderBy(func.desc(func.col("Customer_Frequency")))
)

Frequency_Of_Customer_Visited_To_Restaurant.show()

# Question 10: Find the total sales by each country

Total_Sales_By_Each_Country = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Location").alias("Country"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Amount_Of_Sales"))
    .orderBy(func.desc(func.col("Total_Amount_Of_Sales")))
)

Total_Sales_By_Each_Country.show()

# Question 11: Find the total sales by order source

Total_Sales_By_Order_Source = (
    sales_df.join(menu_df, sales_df["Product_ID"] == menu_df["Product_ID"])
    .groupBy(func.col("Source_order").alias("Source"))
    .agg(func.sum(func.col("Product_price")).alias("Total_Ordered_Amount"))
    .orderBy(func.desc(func.col("Total_Ordered_Amount")))
)

Total_Sales_By_Order_Source.show()
