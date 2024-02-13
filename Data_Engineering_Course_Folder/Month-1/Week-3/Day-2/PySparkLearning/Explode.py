from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("Explode_Examples").getOrCreate()

arrayArrayData = [
    ("James", [["Java", "Scala", "C++"], ["Spark", "Java"]]),
    ("Michael", [["Spark", "Java", "C++"], ["Spark", "Java"]]),
    ("Robert", [["CSharp", "VB"], ["Spark", "Python"]]),
]

df = spark.createDataFrame(data=arrayArrayData, schema=["Name", "Languages"])
df.printSchema()
df.show(truncate=False)

df2 = df.select(func.col("Name"), func.explode("Languages"))
df2.show(truncate=False)

df3 = df.select(func.col("Name"), func.flatten("Languages"))
df3.show(truncate=False)
