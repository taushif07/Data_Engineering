from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("RDD_Test_Application").getOrCreate()

data = [1,2,3,4,5,6,7,8,9]

rdd = spark.sparkContext.parallelize(data)

rdd2 = spark.sparkContext.textFile("./PySparkNotes.txt")

rdd3 = spark.sparkContext.wholeTextFiles("./PySparkNotes.txt")

rdd4 = spark.sparkContext.emptyRDD

rdd5 = spark.sparkContext.parallelize([],10) #This creates 10 partitions


print("rdd1",rdd, "partition of rdd1 is" + str(rdd.getNumPartitions()))
print("rdd2",rdd2, "partition of rdd1 is" + str(rdd2.getNumPartitions()))
print("rdd3",rdd3, "partition of rdd2 is" + str(rdd3.getNumPartitions()))
print("rdd4",rdd4)
print("rdd5",rdd5, "partition of rdd4 is" + str(rdd5.getNumPartitions()))




newRdd = spark.sparkContext.textFile("/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-1/Day-1/PySpark_Learning/PySparkNotes.txt")

newRdd2 = newRdd.flatMap(lambda x: x.split(" "))

print(newRdd2)