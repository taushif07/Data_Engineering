from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import (
    from_json,
    to_json,
    json_tuple,
    col,
    get_json_object,
    schema_of_json,
    lit,
)

spark = SparkSession.builder.appName("Json_Examples").getOrCreate()

jsonString = (
    """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
)
df = spark.createDataFrame([(1, jsonString)], ["id", "value"])
df.show(truncate=False)

# Convert JSON string column to Map type

df2 = df.withColumn("value", from_json(df.value, MapType(StringType(), StringType())))
df2.printSchema()
df2.show(truncate=False)

df2.withColumn("value", to_json(col("value"))).show(truncate=False)

df.select(col("id"), json_tuple(col("value"), "Zipcode", "ZipCodeType", "City")).toDF(
    "id", "Zipcode", "ZipCodeType", "City"
).show(truncate=False)

df.select(
    col("id"), get_json_object(col("value"), "$.ZipCodeType").alias("ZipCodeType")
).show(truncate=False)

schemaStr = (
    spark.range(1)
    .select(
        schema_of_json(
            lit(
                """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
            )
        )
    )
    .collect()[0][0]
)
print(schemaStr)
