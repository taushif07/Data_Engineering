from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    TimestampType,
    StringType,
)
from datetime import datetime

spark = SparkSession.builder.appName("Assignment_1").getOrCreate()


def ParseDateAndTime(dt_str):
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


data = [
    (6696902, "2024-02-02 10:30:00", "2024-02-02 18:17:00", "xfbxcvbx"),
    (8535098, "2024-02-02 09:30:00", "2024-02-02 18:17:00", "xfbxcvbx"),
    (8858051, "2024-02-02 19:30:00", "2024-02-02 22:30:00", "xfbxcvbx"),
]

newData = [
    (newData[0], ParseDateAndTime(newData[1]), ParseDateAndTime(newData[2]), newData[3])
    for newData in data
]


schema = StructType(
    [
        StructField("ID", IntegerType(), True),
        StructField("Start_Date", TimestampType(), True),
        StructField("End_Date", TimestampType(), True),
        StructField("Data", StringType(), True),
    ]
)

df = spark.createDataFrame(newData, schema=schema)

df_hourly = df.withColumn(
    "Hourly_Interval", expr("explode(sequence(Start_Date, End_Date, Interval 1 Hour))")
).selectExpr(
    "ID",
    "Hourly_Interval as Start_Date",
    "Hourly_Interval + Interval '1' Hour as End_Date",
    "Data",
)

df_hourly.show(truncate=False)
