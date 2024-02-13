from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[5]").appName("Fill_Practice").getOrCreate()

df = spark.read.options(header=True, inferSchema=True).csv("./Fill.csv")

df.printSchema()

df.show(truncate=False)

df.na.fill(value=0).show(truncate=False)

df.na.fill(value=0, subset=["population"]).show()

df.na.fill("Not Available", ["type"]).show()

df.na.fill("Unknown", ["city"]).show()

df.na.fill({"type": "NOT AVAILABLE", "city": "UNKNOWN", "population":0, "state":"NA", "zipcode":0}).show()
