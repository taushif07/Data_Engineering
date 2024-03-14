from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder.appName("Total_Amount_Of_Sales_Each_Month").getOrCreate()

merged_sales_data_schema = StructType(
    [
        StructField("Product_ID", IntegerType(), True),
        StructField("Customer_ID", StringType(), True),
        StructField("Order_date", DateType(), True),
        StructField("Location", StringType(), True),
        StructField("Source_order", StringType(), True),
        StructField("Product_name", StringType(), True),
        StructField("Product_price", StringType(), True),
    ]
)


def Total_Amount_Of_Sales_Each_Month_Function(
    merged_sales_data_path, Total_Amount_Of_Sales_Each_Month_Path
):
    merged_sales_data_df = spark.read.csv(
        merged_sales_data_path,
        inferSchema=True,
        schema=merged_sales_data_schema,
    )
    merged_sales_data_df.show(truncate=False)
    Total_Amount_Of_Sales_Each_Month = (
        merged_sales_data_df.withColumn("Month", func.month(func.col("Order_date")))
        .groupBy(func.col("Month").alias("Order_Month"))
        .agg(func.sum("Product_price").alias("Total_Amount_Spent"))
        .orderBy(func.asc("Order_Month"))
    )
    Total_Amount_Of_Sales_Each_Month.show(truncate=False)
    Total_Amount_Of_Sales_Each_Month.write.csv(Total_Amount_Of_Sales_Each_Month_Path)


if __name__ == "__main__":
    merged_sales_data_path = "../sales_csv/merged_sales_data_csv"
    Total_Amount_Of_Sales_Each_Month_Path = (
        "../sales_csv/Total_Amount_Of_Sales_Each_Month"
    )
    Total_Amount_Of_Sales_Each_Month_Function(
        merged_sales_data_path, Total_Amount_Of_Sales_Each_Month_Path
    )
