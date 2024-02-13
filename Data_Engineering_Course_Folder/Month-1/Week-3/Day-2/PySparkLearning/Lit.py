from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.master("local[5]").appName("Lit_Examples").getOrCreate()

data = [("111", 50000), ("222", 60000), ("333", 40000)]
columns = ["EmpId", "Salary"]

df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()
df.show(truncate=False)

df2 = df.select(
    func.col("EmpId"), func.col("Salary"), func.lit("1").alias("Lit_Value1")
)
df2.show(truncate=False)

df3 = df2.withColumn(
    "Lit_Value2",
    func.when(
        (func.col("Salary") >= 20000) & (func.col("Salary") <= 40000),
        func.lit("Below_Average").alias("Salary_Category"),
    )
    .when(
        (func.col("Salary") > 40000) & (func.col("Salary") <= 50000),
        func.lit("Average").alias("Salary_Category"),
    )
    .otherwise(func.lit("Above_Average").alias("Salary_Category")),
)

df3.show(truncate=False)
