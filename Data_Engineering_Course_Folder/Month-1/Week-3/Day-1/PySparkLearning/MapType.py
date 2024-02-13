from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, MapType

spark = (
    SparkSession.builder.master("local[5]").appName("MapType_Examples").getOrCreate()
)

dataDictionary = [
    ("James", {"hair": "black", "eye": "brown"}),
    ("Michael", {"hair": "brown", "eye": None}),
    ("Robert", {"hair": "red", "eye": "black"}),
    ("Washington", {"hair": "grey", "eye": "grey"}),
    ("Jefferson", {"hair": "brown", "eye": ""}),
]

dataSchema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("Properties", MapType(StringType(), StringType()), True),
    ]
)

df = spark.createDataFrame(data=dataDictionary, schema=dataSchema)
df.printSchema()
df.show(truncate=False)


df2 = df.withColumn("Eyes", df.Properties["eye"]).withColumn(
    "hair", df.Properties["hair"]
)
df2.show(truncate=False)

df3 = df2.drop("Properties")
df3.show(truncate=False)
