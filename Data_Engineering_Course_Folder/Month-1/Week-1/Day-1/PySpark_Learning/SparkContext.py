from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

spark = (
    SparkSession.builder.master("local[1]")
    .appName("sparkContextLearning")
    .getOrCreate()
)

print(spark.sparkContext)
print("App name is: " + spark.sparkContext.appName)

spark.sparkContext.stop()
# Creating SparkContext prior to PySpark 2.0

sc = SparkContext("local", "Spark Context Test App")
print("sc appname is " + sc.appName)

sc.stop()

config = SparkConf()
config.setMaster("local").setAppName("New Config Context App")
sparkCon = SparkContext.getOrCreate(config)

print(sparkCon.appName)

sparkCon.stop()
