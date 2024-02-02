from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("Airport_Data_Question_Practice")
    .getOrCreate()
)

df = spark.read.csv("./Airports2.csv", header=True, inferSchema=True)

df.show(50)

# Question 5 : Find the Occupancy Rate for Most Popular Routes
