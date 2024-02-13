from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.master("local[5]").appName("Expr_Examples").getOrCreate()

data = [("James", "Bond"), ("Scott", "Varsa")]
df = spark.createDataFrame(data).toDF("col1", "col2")
df.withColumn("Name", expr(" col1 ||','|| col2")).show()

# Using CASE WHEN sql expression
data2 = [("James", "M"), ("Michael", "F"), ("Jen", "")]
columns2 = ["name", "gender"]
df2 = spark.createDataFrame(data=data2, schema=columns2)
df3 = df2.withColumn(
    "gender",
    expr(
        "CASE WHEN gender = 'M' THEN 'Male' "
        + "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"
    ),
)
df3.show()

# Add months from a value of another column
data3 = [("2019-01-23", 1), ("2019-06-24", 2), ("2019-09-20", 3)]
df4 = spark.createDataFrame(data3).toDF("date", "increment")
df4.select(
    df4.date, df4.increment, expr("add_months(date,increment)").alias("inc_date")
).show()

# Providing alias using 'as'
df4.select(
    df4.date, df4.increment, expr("""add_months(date,increment) as inc_date""")
).show()

# Add
df4.select(df4.date, df4.increment, expr("increment + 5 as new_increment")).show()

# Using cast to convert data types
df4.select(
    "increment", expr("cast(increment as string) as str_increment")
).printSchema()

# Use expr()  to filter the rows
data4 = [(100, 2), (200, 3000), (500, 500)]
df5 = spark.createDataFrame(data4).toDF("col1", "col2")
df5.filter(expr("col1 == col2")).show()
