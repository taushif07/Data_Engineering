from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("ToDate_Examples").getOrCreate()

df = spark.createDataFrame(
    data=[("1", "2019-06-24 12:01:19.000")], schema=["id", "input_timestamp"]
)
df.printSchema()


# Using Cast to convert Timestamp String to DateType
df.withColumn("date_type", col("input_timestamp").cast("date")).show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn("date_type", to_timestamp("input_timestamp").cast("date")).show(
    truncate=False
)

df.select(to_date(lit("06-24-2019 12:01:19.000"), "MM-dd-yyyy HH:mm:ss.SSSS")).show()

# Timestamp String to DateType
df.withColumn("date_type", to_date("input_timestamp")).show(truncate=False)

# Timestamp Type to DateType
df.withColumn("date_type", to_date(current_timestamp())).show(truncate=False)

df.withColumn("ts", to_timestamp(col("input_timestamp"))).withColumn(
    "datetype", to_date(col("ts"))
).show(truncate=False)
