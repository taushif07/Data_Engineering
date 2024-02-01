from pyspark import SparkContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("RDD_leaning_Day2").getOrCreate()

data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]

rdd1 = spark.sparkContext.parallelize(data, 5)
rdd1_val = rdd1.collect()
print("Rdd1 val is ", rdd1_val)

rdd2 = rdd1.map(lambda x: (x * 2))
rdd2_val = rdd2.collect()
print("Rdd2 val is ", rdd2_val)


testRdd = spark.sparkContext.textFile("./PySparkLearning.txt")

testRdd2 = testRdd.flatMap(lambda x: x.split(" "))
testRdd2_value = testRdd2.collect()
testRdd2_count = len(testRdd2_value)

print("testRdd2 val is " +  str(testRdd2_value), "count " + str(testRdd2_count))

testRdd3 = testRdd2.map( lambda el : (el,1))
testRdd3_value = testRdd3.collect()
testRdd3_count = len(testRdd3_value)

print("testRdd3 value is " + str(testRdd3_value), "count " + str(testRdd3_count))


testRdd4 = testRdd3.reduceByKey(lambda a,b: a + b)
testRdd4_value = testRdd4.collect()
testRdd4_count = len(testRdd4_value)

print("testRdd4 value is " + str(testRdd4_value), "count " + str(testRdd4_count))

testRdd5 = testRdd4.map(lambda x: (x[1],x[0]))
testRdd5_value = testRdd5.collect()
testRdd5_count = len(testRdd5_value)

print("testRdd5 value is " + str(testRdd5_value), "count " + str(testRdd5_count))

testRdd6 = testRdd5.sortByKey()
testRdd6_value = testRdd6.collect()
testRdd6_count = len(testRdd6_value)

print("testRdd6 value is " + str(testRdd6_value), "count " + str(testRdd6_count))

testRdd7 = testRdd3.filter(lambda x : 'RDD' in x[0])
testRdd7_value = testRdd7.collect()
testRdd7_count = len(testRdd7_value)
print("testRdd7 value is " + str(testRdd7_value), "count " + str(testRdd7_count))

# first() – Returns the first record

firstRec = testRdd6.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1], firstRec)

# max() – Returns max record.

datMax = testRdd6.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1], datMax)

# reduce() – Reduces the records to single, we can use this to count or sum

totalWordCount = testRdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]), totalWordCount)

# take() – Returns the record specified as an argument.

data3 = testRdd6.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])

print("data3 data is " + str(data3))
# collect() – Returns all data from RDD as an array. 
# Be careful when you use this action when you are working with huge RDD with millions 
# and billions of data as you may run out of memory on the driver.

data = testRdd6.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])

# saveAsTextFile() – Using saveAsTestFile action, we can write the RDD to a text file.

# testRdd6.saveAsTextFile("/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-1/Day-2/PySparkLearning")

# Optimise code for the above operations :

newRdd = spark.sparkContext.textFile("./PySparkLearning.txt")
newRdd_val = newRdd.collect()
newRdd2 = (
    newRdd.flatMap(lambda x: x.split(" "))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda x: (x[1], x[0]))
    .sortByKey()
)
newRdd_Split_val = newRdd2.collect()
print("Rdd3 val is ", newRdd_val)
print("Rdd4 val is " + str(newRdd_Split_val))
