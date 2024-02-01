from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("firstTestApp").getOrCreate()

# Create a DataFrame
data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
columns = ["Language", "Users"]
df = spark.createDataFrame(data, schema=columns)

newColumns = ["ID", "FirstName", "LastName", "Gender", "DOB", "Email", "Salary"]
newData = [
    (1, "Taushif", "Ansari", "Male", "16th oct 99", "tau@yopmail.com", 14200),
    (2, "Mirul", "Ansari", "Male", "16th oct 99", "tau@yopmail.com", 7000),
    (3, "Raazi", "Ahmad", "Male", "16th oct 99", "tau@yopmail.com", 10000),
    (4, "Dev", "Basak", "Male", "16th oct 99", "tau@yopmail.com", 20000),
    (5, "Zeenat", "Parween", "Female", "16th oct 99", "tau@yopmail.com", 8000),
]

newDf = spark.createDataFrame(newData, schema=newColumns)

# Show the DataFrame
df.show()
newDf.show()

# Stop the SparkSession
spark.stop()
