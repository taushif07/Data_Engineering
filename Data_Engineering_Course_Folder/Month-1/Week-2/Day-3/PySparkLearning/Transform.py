from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, transform

spark = (
    SparkSession.builder.master("local[5]").appName("Transform_Examples").getOrCreate()
)

simpleData = (
    ("Java", 4000, 5),
    ("Python", 4600, 10),
    ("Scala", 4100, 15),
    ("Scala", 4500, 15),
    ("PHP", 3000, 20),
)
columns = ["CourseName", "Fee", "Discount"]

df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)


def toUpperStringColumn(DF):
    return DF.withColumn("CourseName", upper(DF.CourseName))


def reducePrice(DF, reducePriceBy):
    return DF.withColumn("New_Fee", DF.Fee - reducePriceBy)


def applyDiscount(DF):
    return DF.withColumn(
        "Discounted_Fee", DF.New_Fee - (DF.New_Fee * DF.Discount) / 100
    )


def selectColumn(DF):
    return DF.select("CourseName", "Discounted_Fee")


df2 = (
    df.transform(toUpperStringColumn)
    .transform(reducePrice, 1000)
    .transform(applyDiscount)
)

df2.show(truncate=False)

df3 = (
    df.transform(toUpperStringColumn)
    .transform(reducePrice, 1000)
    .transform(applyDiscount)
    .transform(selectColumn)
)

df3.show(truncate=False)

data = [
    ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"]),
    ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"]),
    ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"]),
]

dataColumn = ["Name", "Languages1", "Languages2"]

df4 = spark.createDataFrame(data=data, schema=dataColumn)
df4.printSchema()
df4.show(truncate=False)

df4.select(transform("Languages1", lambda x: upper(x)).alias("languages1")).show()
