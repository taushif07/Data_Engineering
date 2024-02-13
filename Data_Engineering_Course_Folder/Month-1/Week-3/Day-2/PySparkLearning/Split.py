from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.master("local[5]").appName("Split_Examples").getOrCreate()

data = [
    ("James, A, Smith", "2018", "M", 3000),
    ("Michael, Rose, Jones", "2010", "M", 4000),
    ("Robert,K,Williams", "2010", "M", 4000),
    ("Maria,Anne,Jones", "2005", "F", 4000),
    ("Jen,Mary,Brown", "2010", "", -1),
]

columns = ["Name", "DOB", "Gender", "Salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.select(
    func.split(func.col("Name"), ",").alias("Name_Array"),
    func.col("DOB"),
    func.col("Gender"),
    func.col("Salary"),
)
df2.show(truncate=False)
