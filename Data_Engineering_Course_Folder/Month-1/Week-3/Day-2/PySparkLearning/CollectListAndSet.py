from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local[5]")
    .appName("Collect_list_And_Set_Examples")
    .getOrCreate()
)

simpleData = [
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
]
schema = ["Employee_Name", "Department", "Salary"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

df2 = df.select(collect_list(col("Salary")))
df2.show(truncate=False)

df3 = df.select(collect_set(col("Salary")))
df3.show(truncate=False)
