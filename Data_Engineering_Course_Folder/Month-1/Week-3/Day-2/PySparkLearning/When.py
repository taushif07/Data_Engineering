from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.master("local[5]").appName("When_Examples").getOrCreate()

data = [
    ("James", "M", 60000),
    ("Michael", "M", 70000),
    ("Robert", None, 400000),
    ("Maria", "F", 500000),
    ("Jen", "", None),
]

dataSchema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=dataSchema)

df.printSchema()
df.show(truncate=False)

df2 = df.withColumn(
    "New_Gender",
    func.when(func.col("Gender") == "M", "Male")
    .when(func.col("Gender") == "F", "Female")
    .when(func.col("Gender").isNull(), "Transgender")
    .otherwise("Not Available"),
)

df2.show(truncate=False)

df3 = df.select(
    func.col("*"),
    func.when(func.col("Gender") == "M", "Male")
    .when(func.col("Gender") == "F", "Female")
    .when(func.col("Gender").isNull(), "Shemale")
    .otherwise("LGBTQ")
    .alias("new_gender"),
)

df3.show(truncate=False)
