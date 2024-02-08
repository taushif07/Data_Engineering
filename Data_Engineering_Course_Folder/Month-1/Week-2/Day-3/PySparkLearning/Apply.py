from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Apply_Examples").getOrCreate()

columns = ["Seqno", "Name"]
data = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders")]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)

df.withColumn("Upper_Name", upper(df.Name)).show()

df.select("Seqno", "Name", upper(df.Name)).show()


def upperCase(str):
    return str.upper()


upperCaseUDF = udf(lambda x: upperCase(x), StringType())

df.withColumn("Cureated Name", upperCaseUDF(col("Name"))).show(truncate=False)

df.select(col("Seqno"), upperCaseUDF(col("Name")).alias("Name")).show(truncate=False)
