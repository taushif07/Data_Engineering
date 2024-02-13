from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("CreateMap_Examples").getOrCreate()

data = [
    ("36636", "Finance", 3000, "USA"),
    ("40288", "Finance", 5000, "IND"),
    ("42114", "Sales", 3900, "USA"),
    ("39192", "Marketing", 2500, "CAN"),
    ("34534", "Sales", 6500, "USA"),
]
schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("Department", StringType(), True),
        StructField("Salary", IntegerType(), True),
        StructField("Location", StringType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn(
    "Properties_Map",
    create_map(lit("Salary"), col("Salary"), lit("Location"), col("Location")),
).drop("Salary", "Location")

df2.show(truncate=False)
