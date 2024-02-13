from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.master("local[5]")
    .appName("SumAndAverage_Examples")
    .getOrCreate()
)

simpleData = [
    ("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "NV", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Raman", "Finance", "DE", 99000, 40, 24000),
    ("Scott", "Finance", "NY", 83000, 36, 19000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "NV", 80000, 25, 18000),
    ("Kumar", "Marketing", "NJ", 91000, 50, 21000),
]

schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)


dfGroup = df.groupBy("department").agg(sum("bonus").alias("sum_salary"))

dfGroup.show(truncate=False)
