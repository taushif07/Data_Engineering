from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

spark = SparkSession.builder.appName("Substring_Examples").getOrCreate()
data = [(1, "20200828"), (2, "20180525")]
columns = ["ID", "Date"]
df = spark.createDataFrame(data, columns)

# Using SQL function substring()
df.withColumn("year", substring("Date", 1, 4)).withColumn(
    "month", substring("Date", 5, 2)
).withColumn("day", substring("Date", 7, 2))
df.printSchema()
df.show(truncate=False)

# Using select
df1 = df.select(
    "Date",
    substring("Date", 1, 4).alias("year"),
    substring("Date", 5, 2).alias("month"),
    substring("Date", 7, 2).alias("day"),
)

# Using with selectExpr
df2 = df.selectExpr(
    "Date",
    "substring(Date, 1,4) as year",
    "substring(Date, 5,2) as month",
    "substring(Date, 7,2) as day",
)

# Using substr from Column type
df3 = (
    df.withColumn("year", col("Date").substr(1, 4))
    .withColumn("month", col("Date").substr(5, 2))
    .withColumn("day", col("Date").substr(7, 2))
)

df1.show()
df2.show()
df3.show()
