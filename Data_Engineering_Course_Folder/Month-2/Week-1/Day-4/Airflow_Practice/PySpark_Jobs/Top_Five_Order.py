from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder.appName("Top_Five_Order").getOrCreate()

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


def Top_Five_Order_Function(merged_sales_data_path, top_five_order_data_path):
    merged_sales_data_df = spark.read.csv(
        merged_sales_data_path,
        inferSchema=True,
        schema=merged_sales_data_schema,
    )
    merged_sales_data_df.show(truncate=False)
    Top_Five_Order = (
        merged_sales_data_df.groupBy(func.col("Product_ID"), func.col("Product_name"))
        .agg(func.count(func.col("Product_ID")).alias("Total_Items_Purchased"))
        .orderBy(func.desc(func.col("Total_Items_Purchased")))
        .limit(5)
    )
    Top_Five_Order.show(truncate=False)
    Top_Five_Order.write.csv(top_five_order_data_path)


if __name__ == "__main__":
    merged_sales_data_path = "../sales_csv/merged_sales_data_csv"
    top_five_order_data_path = "../sales_csv/Top_Five_Order_csv"
    Top_Five_Order_Function(merged_sales_data_path, top_five_order_data_path)
