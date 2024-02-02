from pyspark.sql import SparkSession, functions as func

spark = (
    SparkSession.builder.master("local")
    .appName("Google_PlayStore_User_Review")
    .getOrCreate()
)

df = spark.read.csv("./googleplaystore_user_reviews.csv", header=True, inferSchema=True)

df.printSchema()
df.show(10)
