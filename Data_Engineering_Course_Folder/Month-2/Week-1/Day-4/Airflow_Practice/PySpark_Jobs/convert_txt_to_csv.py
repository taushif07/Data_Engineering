from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType

spark = SparkSession.builder.appName("Convert_txt_to_csv").getOrCreate()

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


def convert_txt_to_csv(sales_txt_path, menu_txt_path, sales_csv_path, menu_csv_path):
    sales_df = spark.read.csv(sales_txt_path, inferSchema=True, schema=sales_schema)
    menu_df = spark.read.csv(menu_txt_path, inferSchema=True, schema=menu_schema)
    sales_df.write.csv(sales_csv_path)
    menu_df.write.csv(menu_csv_path)


if __name__ == "__main__":
    sales_txt_path = "../txt/sales.csv.txt"
    menu_txt_path = "../txt/menu.csv.txt"
    sales_csv_path = "../sales_csv/sales_csv"
    menu_csv_path = "../sales_csv/menu_csv"
    convert_txt_to_csv(sales_txt_path, menu_txt_path, sales_csv_path, menu_csv_path)
