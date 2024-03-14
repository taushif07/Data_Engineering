from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

spark = SparkSession.builder.appName(
    "Total_Amount_Spent_On_Each_Food_Category"
).getOrCreate()

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


def Total_Amount_Spent_Per_Food_Category_Function(
    merged_sales_data_path, Total_Amount_Spent_Per_Food_Category_Path
):
    merged_sales_data_df = spark.read.csv(
        merged_sales_data_path,
        inferSchema=True,
        schema=merged_sales_data_schema,
    )
    merged_sales_data_df.show(truncate=False)
    Total_Amount_Spent_Per_Food_Category = (
        merged_sales_data_df.groupBy(func.col("Product_name"))
        .agg(func.sum("Product_price").alias("Total_Amount_Spent"))
        .orderBy(func.desc("Total_Amount_Spent"))
    )
    Total_Amount_Spent_Per_Food_Category.show(truncate=False)
    Total_Amount_Spent_Per_Food_Category.write.csv(
        Total_Amount_Spent_Per_Food_Category_Path
    )


if __name__ == "__main__":
    merged_sales_data_path = "../sales_csv/merged_sales_data_csv"
    Total_Amount_Spent_Per_Food_Category_Path = (
        "../sales_csv/Total_Amount_Spent_On_Each_Food_Category_CSV"
    )
    Total_Amount_Spent_Per_Food_Category_Function(
        merged_sales_data_path, Total_Amount_Spent_Per_Food_Category_Path
    )
