from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("SQLDateAndTimeStamp_Examples").getOrCreate()
data = [["1", "2020-02-01"], ["2", "2019-03-01"], ["3", "2021-03-01"]]
df = spark.createDataFrame(data, ["id", "input"])
df.show()

# current_date()

currDateDF = df.select(current_date().alias("Current_Date"))
currDateDF.show(1)

# date_format()

dateFormatDF = df.select(
    col("input"), date_format(col("input"), "MM-dd-yyyy").alias("Formatted_Date")
)
dateFormatDF.show()

# to_date()

toDateDF = df.select(col("input"), to_date(col("input"), "yyy-MM-dd").alias("to_date"))
toDateDF.show()


# datediff()

dateDifDF = df.select(
    col("input"), datediff(current_date(), col("input")).alias("datediff")
)
dateDifDF.show()

# months_between()

monthDifDF = df.select(
    col("input"), months_between(current_date(), col("input")).alias("months_between")
)

monthDifDF.show()

# trunc()

truncDF = df.select(
    col("input"),
    trunc(col("input"), "Month").alias("Month_Trunc"),
    trunc(col("input"), "Year").alias("Month_Year"),
    trunc(col("input"), "Month").alias("Month_Trunc"),
)

truncDF.show()

# add_months() , date_add(), date_sub()

addMonthDF = df.select(
    col("input"),
    add_months(col("input"), 3).alias("add_months"),
    add_months(col("input"), -3).alias("sub_months"),
    date_add(col("input"), 4).alias("date_add"),
    date_sub(col("input"), 4).alias("date_sub"),
)

addMonthDF.show()

# year(), month(), month(),next_day(), weekofyear()

YearDF = df.select(
    col("input"),
    year(col("input")).alias("year"),
    month(col("input")).alias("month"),
    next_day(col("input"), "Sunday").alias("next_day"),
    weekofyear(col("input")).alias("weekofyear"),
)
YearDF.show()

# dayofweek(), dayofmonth(), dayofyear()

dayDF = df.select(
    col("input"),
    dayofweek(col("input")).alias("dayofweek"),
    dayofmonth(col("input")).alias("dayofmonth"),
    dayofyear(col("input")).alias("dayofyear"),
)

dayDF.show()

# current_timestamp()
data2 = [
    ["1", "02-01-2020 11 01 19 06"],
    ["2", "03-01-2019 12 01 19 406"],
    ["3", "03-01-2021 12 01 19 406"],
]
df2 = spark.createDataFrame(data2, ["id", "input"])
df2.show(truncate=False)

df2.select(current_timestamp().alias("current_timestamp")).show(1, truncate=False)

# to_timestamp()
df2.select(
    col("input"),
    to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp"),
).show(truncate=False)

# hour, minute,second
data3 = [
    ["1", "2020-02-01 11:01:19.06"],
    ["2", "2019-03-01 12:01:19.406"],
    ["3", "2021-03-01 12:01:19.406"],
]
df3 = spark.createDataFrame(data3, ["id", "input"])

df3.select(
    col("input"),
    hour(col("input")).alias("hour"),
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second"),
).show(truncate=False)
