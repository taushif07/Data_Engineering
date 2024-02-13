from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    rank,
    dense_rank,
    percent_rank,
    ntile,
    cume_dist,
    lag,
    lead,
    avg,
    sum,
    min,
    max,
)
from pyspark.sql.types import StructField, StringType, StructType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[5]").appName("Window_Examples").getOrCreate()

simpleData = (
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
)

simpleDataSchema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=simpleData, schema=simpleDataSchema)
df.printSchema()
df.show(truncate=False)

windowData = Window.partitionBy(col("Department")).orderBy(col("Salary"))

# row_number()

rowNumberDF = df.withColumn("Row_Number", row_number().over(windowData))
rowNumberDF.show(truncate=False)

# rank()

rankDF = df.withColumn("Rank", rank().over(windowData))
rankDF.show(truncate=False)

# dense_rank()

denseRankDF = df.withColumn("Dense_Rank", dense_rank().over(windowData))
denseRankDF.show(truncate=False)

# percent_rank

percentRank = df.withColumn("Percentage_Rank", percent_rank().over(windowData))
percentRank.show(truncate=False)

# ntile()

NtileDF = df.withColumn("Ntile", ntile(2).over(windowData))
NtileDF.show(truncate=False)

# cume_dist()

CumeDistDF = df.withColumn("Cume_Dist", cume_dist().over(windowData))
CumeDistDF.show(truncate=False)

# lag()

lagDF = df.withColumn("Lag", lag("Salary", 2).over(windowData))
lagDF.show(truncate=False)

# lead()

leadDf = df.withColumn("Lead", lead("Salary", 2).over(windowData))
leadDf.show(truncate=False)

# Aggregate fumctions
windowDataAgg = Window.partitionBy("department")

aggFunction = (
    df.withColumn("row", row_number().over(windowData))
    .withColumn("avg", avg(col("salary")).over(windowDataAgg))
    .withColumn("sum", sum(col("salary")).over(windowDataAgg))
    .withColumn("min", min(col("salary")).over(windowDataAgg))
    .withColumn("max", max(col("salary")).over(windowDataAgg))
    .where(col("row") == 1)
    .select("department", "avg", "sum", "min", "max")
)

aggFunction.show(truncate=False)
