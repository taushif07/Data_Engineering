from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, MapType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("MapkeysAndValues_Examples").getOrCreate()

dataDictionary = [
    ("James", {"hair": "black", "eye": "brown"}),
    ("Michael", {"hair": "brown", "eye": None}),
    ("Robert", {"hair": "red", "eye": "black"}),
    ("Washington", {"hair": "grey", "eye": "grey"}),
    ("Jefferson", {"hair": "brown", "eye": ""}),
]

schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]
)

df = spark.createDataFrame(data=dataDictionary, schema=schema)
df.printSchema()
df.show(truncate=False)

df2 = df.select(col("name"), map_keys(col("properties")))
df2.show(truncate=False)

df3 = df.select(col("name"), map_values(col("properties")))
df3.show(truncate=False)
