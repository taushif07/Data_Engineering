from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("TimeStamp_Examples").getOrCreate()

df = spark.createDataFrame(
    data=[("1", "2019-06-24 12:01:19.000")], schema=["id", "input_timestamp"]
)
df.printSchema()

# Timestamp String to DateType
df.withColumn("timestamp", to_timestamp("input_timestamp")).show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn("timestamp", to_timestamp("input_timestamp").cast("string")).show(
    truncate=False
)


df.select(
    to_timestamp(lit("06-24-2019 12:01:19.000"), "MM-dd-yyyy HH:mm:ss.SSSS")
).show(truncate=False)
