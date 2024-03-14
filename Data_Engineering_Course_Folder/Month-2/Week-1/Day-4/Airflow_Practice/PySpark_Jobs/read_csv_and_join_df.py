from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DateType

spark = SparkSession.builder.appName("read_csv_and_join_df").getOrCreate()

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


def read_csv_and_join_df(sales_csv_path, menu_csv_path, merged_sales_data_path):
    sales_csv_df = spark.read.csv(sales_csv_path, inferSchema=True, schema=sales_schema)
    menu_csv_df = spark.read.csv(menu_csv_path, inferSchema=True, schema=menu_schema)
    sales_csv_df.show(truncate=False)
    menu_csv_df.show(truncate=False)
    df_based_on_productId = sales_csv_df.join(
        menu_csv_df, sales_csv_df["Product_ID"] == menu_csv_df["Product_ID"]
    ).drop(menu_csv_df["Product_ID"])
    df_based_on_productId.show(truncate=False)
    df_based_on_productId.write.csv(merged_sales_data_path)


if __name__ == "__main__":
    sales_csv_path = "../sales_csv/sales_csv"
    menu_csv_path = "../sales_csv/menu_csv"
    merged_sales_data_path = "../sales_csv/merged_sales_data_csv"
    read_csv_and_join_df(sales_csv_path, menu_csv_path, merged_sales_data_path)
