from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[5]").appName("Partition_Examples").getOrCreate()
)

df = spark.read.options(header=True, inferSchema=True).csv("./Partition.csv")

df.printSchema()
df.show(truncate=False)

df2 = df.write.options(header=True, inferSchema=True).partitionBy("State").mode(
    "overwrite"
).csv("./zipcodes-state")
