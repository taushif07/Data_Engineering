from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local").appName("Show_Function_Practice").getOrCreate()
)

columns = ["Serial No", "Quote"]

data = [
    ("1", "Be the change that you wish to see in the world"),
    (
        "2",
        "Everyone thinks of changing the world, but no one thinks of changing himself.",
    ),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),
]

df = spark.createDataFrame(data, columns)

df.show()
df.show(truncate=False)
df.show(1, truncate=False)
df.show(2, truncate=40)  # default value of truncate is 20
