from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
)

spark = (
    SparkSession.builder.master("local[1]")
    .appName("ReadJsonFile_Examples")
    .getOrCreate()
)

df = spark.read.option("multiline", "true").json("./ZipCode.json")
df.printSchema()
df.show()

multiline_df = spark.read.option("multiline", "true").json("./MultiLineZipCode.json")
multiline_df.show()

# Read multiple files
df2 = spark.read.option("multiline", "true").json(
    ["./ZipCode.json", "./MultiLineZipCode.json"]
)
df2.show()

# Read All JSON files from a directory
df3 = spark.read.option("multiline", "true").json("./*.json")
df3.show()

# Define custom schema
schema = StructType(
    [
        StructField("RecordNumber", IntegerType(), True),
        StructField("Zipcode", IntegerType(), True),
        StructField("ZipCodeType", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("LocationType", StringType(), True),
        StructField("Lat", DoubleType(), True),
        StructField("Long", DoubleType(), True),
        StructField("Xaxis", IntegerType(), True),
        StructField("Yaxis", DoubleType(), True),
        StructField("Zaxis", DoubleType(), True),
        StructField("WorldRegion", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("LocationText", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Decommisioned", BooleanType(), True),
        StructField("TaxReturnsFiled", StringType(), True),
        StructField("EstimatedPopulation", IntegerType(), True),
        StructField("TotalWages", IntegerType(), True),
        StructField("Notes", StringType(), True),
    ]
)

df_with_schema = (
    spark.read.option("multiline", "true").schema(schema).json("./ZipCode.json")
)
df_with_schema.printSchema()
df_with_schema.show()
