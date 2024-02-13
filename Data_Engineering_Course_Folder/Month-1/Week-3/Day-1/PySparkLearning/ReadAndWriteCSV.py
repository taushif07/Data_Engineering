from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import DoubleType, BooleanType

spark = SparkSession.builder.appName("ReadAndWriteCSV_Examples").getOrCreate()

df = spark.read.csv("./ZipCode.csv")

df.printSchema()
df.show(truncate=False)

df2 = spark.read.option("header", True).csv("./ZipCode.csv")
df2.printSchema()
df2.show(truncate=False)

df3 = spark.read.options(header="True", delimiter=",").csv("./ZipCode.csv")
df3.printSchema()
df3.show(truncate=False)

schema = (
    StructType()
    .add("RecordNumber", IntegerType(), True)
    .add("Zipcode", IntegerType(), True)
    .add("ZipCodeType", StringType(), True)
    .add("City", StringType(), True)
    .add("State", StringType(), True)
    .add("LocationType", StringType(), True)
    .add("Lat", DoubleType(), True)
    .add("Long", DoubleType(), True)
    .add("Xaxis", IntegerType(), True)
    .add("Yaxis", DoubleType(), True)
    .add("Zaxis", DoubleType(), True)
    .add("WorldRegion", StringType(), True)
    .add("Country", StringType(), True)
    .add("LocationText", StringType(), True)
    .add("Location", StringType(), True)
    .add("Decommisioned", BooleanType(), True)
    .add("TaxReturnsFiled", StringType(), True)
    .add("EstimatedPopulation", IntegerType(), True)
    .add("TotalWages", IntegerType(), True)
    .add("Notes", StringType(), True)
)

df_with_schema = (
    spark.read.format("csv").option("header", True).schema(schema).load("./ZipCode.csv")
)
df_with_schema.printSchema()
df_with_schema.show(truncate=False)

df2.write.option("header", True).csv("./NewZipCode.csv")
