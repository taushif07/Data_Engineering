from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ForEach_Examples").getOrCreate()

data = [("1", "john jones"), ("2", "tracey smith"), ("3", "amy sanders")]
columns = ["Seqno", "Name"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()


def f(df):
    print(df.Seqno)


df.foreach(f)

accum = spark.sparkContext.accumulator(0)
df.foreach(lambda x: accum.add(int(x.Seqno)))
print(accum.value)
