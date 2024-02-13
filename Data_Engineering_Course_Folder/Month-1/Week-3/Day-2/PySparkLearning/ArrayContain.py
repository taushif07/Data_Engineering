from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local[5]")
    .appName("Arraycontain_Examples")
    .getOrCreate()
)

data = [
    ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
    ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
    ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV"),
]

schema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("LanguagesAtSchool", ArrayType(StringType()), True),
        StructField("LanguagesAtWork", ArrayType(StringType()), True),
        StructField("CurrentState", StringType(), True),
        StructField("PreviousState", StringType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()

df2 = df.select(
    col("Name"),
    array_contains(col("LanguagesAtSchool"), "Java").alias("Java_Is_Available"),
)
df2.show()

df3 = df.select(
    col("Name"), array(col("CurrentState"), col("PreviousState")).alias("State")
)
df3.show()
