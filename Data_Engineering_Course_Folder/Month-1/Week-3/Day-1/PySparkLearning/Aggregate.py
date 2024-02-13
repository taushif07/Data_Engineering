from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = (
    SparkSession.builder.master("local[5]").appName("Aggregate_Examples").getOrCreate()
)

simpleData = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
]

simpleDataSchema = StructType(
    [
        StructField("Names", StringType(), True),
        StructField("Deparment", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=simpleData, schema=simpleDataSchema)

df.printSchema()
df.show(truncate=False)

# approx_count_distinct()
df2 = df.select(func.approx_count_distinct("Salary")).collect()[0][0]
print("Approx count distinct " + str(df2))

# average (avg)

df3 = df.select(func.avg("Salary")).collect()[0][0]
print("average salary " + str(df3))

# collect_list

df4 = df.select(func.collect_list("Salary"))
df4.show(truncate=False)


# collect_set

df5 = df.select(func.collect_set("Salary"))
df5.show(truncate=False)

# countDistinct

df6 = df.select(func.countDistinct("Salary"))
df6.show(truncate=False)
print("Distinct Count of Department & Salary: " + str(df6.collect()[0][0]))

# count
df7 = df.select(func.count("Salary")).collect()[0]
print("count: " + str(df7))

# first
df.select(func.first("Salary")).show(truncate=False)
# last
df.select(func.last("Salary")).show(truncate=False)
# kurtosis
df.select(func.kurtosis("Salary")).show(truncate=False)
# max
df.select(func.max("Salary")).show(truncate=False)
# min
df.select(func.min("Salary")).show(truncate=False)
# mean
df.select(func.mean("Salary")).show(truncate=False)
# skewness
df.select(func.skewness("Salary")).show(truncate=False)
# stddev
df.select(
    func.stddev("Salary"), func.stddev_samp("Salary"), func.stddev_pop("Salary")
).show(truncate=False)
# sum
df.select(func.sum("Salary")).show(truncate=False)
# sum_distinct
df.select(func.sum_distinct("Salary")).show(truncate=False)
# variance
df.select(
    func.variance("Salary"), func.var_samp("Salary"), func.var_pop("Salary")
).show(truncate=False)
