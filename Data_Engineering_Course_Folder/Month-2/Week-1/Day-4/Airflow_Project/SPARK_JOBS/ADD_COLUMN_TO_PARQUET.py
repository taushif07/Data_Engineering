from pyspark.sql import SparkSession, functions as func

# <---------------------------------------------------------------------->

# This is a simple code to read parquet file and add new columns in it
# spark = SparkSession.builder.appName("ADD_COLUMN_TO_PARQUET").getOrCreate()

# df = spark.read.parquet("../PARQUET")

# df = df.withColumn("CEO", func.lit("TAUSHIF ANSARI")).withColumn(
#     "DEVELOPER", func.lit("TAUSHIF ANSARI")
# )

# # df.show(truncate=False)
# df.write.parquet("../MODIFIED_PARQUET")

# <---------------------------------------------------------------------->


def add_columns_and_save(input_path, output_path):
    spark = SparkSession.builder.appName("ADD_COLUMN_TO_PARQUET").getOrCreate()
    df = spark.read.parquet(input_path)
    df = df.withColumn("CEO", func.lit("TAUSHIF ANSARI")).withColumn(
        "DEVELOPER", func.lit("TAUSHIF ANSARI")
    )
    df.write.parquet(output_path)


if __name__ == "__main__":
    input_path = "../PARQUET"
    output_path = "../MODIFIED_PARQUET"
    add_columns_and_save(input_path, output_path)
