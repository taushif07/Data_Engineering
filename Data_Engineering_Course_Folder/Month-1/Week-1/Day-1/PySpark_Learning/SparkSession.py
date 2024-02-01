from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark_Session_Demo").getOrCreate()

print("spark1", spark)

# creating another spark session

newSpark = SparkSession.newSession

print("newSpark", newSpark)

# getting existing spark session

existSpark = SparkSession.builder.getOrCreate

print("existSpark", existSpark)

# Using Spark Config

# configSpark = SparkSession.builder.appName('Spark_config').config("spark.sql.warehouse.dir", "<path>/spark-warehouse").enableHiveSupport()getOrCreate()

# print('configSpark', configSpark)

# Once the SparkSession is created, you can add the spark configs during runtime or get all configs.

# configSpark.Config.set("spark.executor.memory", '5G')

# partitions = configSpark.Config.get("spark.sql.shuffle.partitions")

# print(partitions)


spark.stop()
