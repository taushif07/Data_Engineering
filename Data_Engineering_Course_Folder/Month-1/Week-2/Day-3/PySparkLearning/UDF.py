from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF_Examples").getOrCreate()

columns = ["Seqno", "Name"]
data = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders")]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)


def convertToUpperCase(str):
    resStr = ""
    arr = str.split(" ")
    for x in arr:
        resStr = resStr + x[0:1].upper() + x[1 : len(x)] + " "
    return resStr


convertUDF = udf(lambda z: convertToUpperCase(z))

df.select(col("Seqno"), convertUDF(col("Name")).alias("Name")).show(truncate=False)


def upperCase(str):
    return str.upper()


upperCaseUDF = udf(lambda z: upperCase(z), StringType())

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))).show(truncate=False)

columns = ["Seqno", "Name"]
data2 = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders"), ("4", None)]

df2 = spark.createDataFrame(data=data2, schema=columns)
df2.show(truncate=False)
df2.createOrReplaceTempView("NAME_TABLE2")

spark.udf.register(
    "_nullsafeUDF",
    lambda str: (str) if not str is None else "",
    StringType(),
)

spark.sql("select _nullsafeUDF(Name) from NAME_TABLE2").show(truncate=False)
