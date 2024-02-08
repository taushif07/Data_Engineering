from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local")
    .appName("PySpark_Accumulator_learning")
    .getOrCreate()
)

accumulte = spark.sparkContext.accumulator(0)

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

rdd.foreach(lambda x: accumulte.add(x))

print(accumulte.value)

# Sum based on function

accumulateSum = spark.sparkContext.accumulator(0)


def accSumFunc(x):
    global accumulateSum
    accumulateSum += x


rdd.foreach(accSumFunc)

print(accumulateSum.value)


# count

accumCount = spark.sparkContext.accumulator(0)

rdd.foreach(lambda x: accumCount.add(1))

print(accumCount.value)
