from pyspark.sql import SparkSession

# <---------------------------------------------------------------------->
# this is the simple code to convert csv to parquet
# spark = SparkSession.builder.appName("CSV_TO_PARQUET").getOrCreate()

# df = spark.read.csv("../CSV/googleplaystore.csv", header=True, inferSchema=True)

# df.write.parquet("../PARQUET")
# <---------------------------------------------------------------------->


def convert_csv_to_parquet(input_path, output_path):
    spark = SparkSession.builder.appName("CSV_TO_PARQUET").getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.write.parquet(output_path)


if __name__ == "__main__":
    input_path = "../CSV/googleplaystore.csv"
    output_path = "../PARQUET"
    convert_csv_to_parquet(input_path, output_path)
