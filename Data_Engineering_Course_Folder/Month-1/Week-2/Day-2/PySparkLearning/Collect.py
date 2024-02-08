from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Collect_Examples").getOrCreate()

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.show(truncate=False)

dataCollect = deptDF.collect()
print(dataCollect)

for row in dataCollect:
    print(row["dept_name"] + "," + str(row["dept_id"]))


print(deptDF.collect()[0][0])
