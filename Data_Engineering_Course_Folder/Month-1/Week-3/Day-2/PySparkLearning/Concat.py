from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

spark = SparkSession.builder.appName("Concat_Examples").getOrCreate()

columns = ["name", "languagesAtSchool", "currentState"]
data = [
    ("James,,Smith", ["Java", "Scala", "C++"], "CA"),
    ("Michael,Rose,", ["Spark", "Java", "C++"], "NJ"),
    ("Robert,,Williams", ["CSharp", "VB"], "NV"),
]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("languagesAtSchool", concat_ws(",", col("languagesAtSchool")))
df2.printSchema()
df2.show(truncate=False)
