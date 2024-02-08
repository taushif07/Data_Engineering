from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UnionByName_Examples").getOrCreate()

# Create DataFrame df1 with columns name, and id
data = [("James", 34), ("Michael", 56), ("Robert", 30), ("Maria", 24)]

df1 = spark.createDataFrame(data=data, schema=["name", "id"])
df1.printSchema()

# Create DataFrame df2 with columns name and id
data2 = [(34, "James"), (45, "Maria"), (45, "Jen"), (34, "Jeff")]

df2 = spark.createDataFrame(data=data2, schema=["id", "name"])
df2.printSchema()

# Using unionByName()
df3 = df1.unionByName(df2)
df3.printSchema()
df3.show()

# Using allowMissingColumns
df4 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df5 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df6 = df4.unionByName(df5, allowMissingColumns=True)
df6.printSchema()
df6.show()
