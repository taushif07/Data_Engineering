from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    MapType,
)
from pyspark.sql.functions import col, struct, when

spark = (
    SparkSession.builder.master("local[1]")
    .appName("StructType_StructField_Examples")
    .getOrCreate()
)

data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1),
]

dataSchema = StructType(
    [
        StructField("First_Name", StringType(), True),
        StructField("Middle_Name", StringType(), True),
        StructField("Last_Name", StringType(), True),
        StructField("ID", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ]
)

df = spark.createDataFrame(data=data, schema=dataSchema)
df.printSchema()
df.show(truncate=False)


structureData = [
    (("James", "", "Smith"), "36636", "M", 3100),
    (("Michael", "Rose", ""), "40288", "M", 4300),
    (("Robert", "", "Williams"), "42114", "M", 1400),
    (("Maria", "Anne", "Jones"), "39192", "F", 5500),
    (("Jen", "Mary", "Brown"), "", "F", -1),
]


structureDataSchema = StructType(
    [
        StructField(
            "Name",
            StructType(
                [
                    StructField("First_Name", StringType(), True),
                    StructField("Middle_Name", StringType(), True),
                    StructField("Last_Name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("ID", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Salary", IntegerType(), True),
    ]
)

df2 = spark.createDataFrame(data=structureData, schema=structureDataSchema)
df2.printSchema()
df2.show(truncate=False)
print(df.schema.json())

updateDF2 = df2.withColumn(
    "Other_Info",
    struct(
        col("ID").alias("Identifier"),
        col("Gender").alias("Gender"),
        col("Salary").alias("Salary"),
        when(col("Salary").cast(IntegerType()) < 2000, "Low")
        .when(col("Salary").cast(IntegerType()) < 5000, "Medium")
        .otherwise("High")
        .alias("Salary_Grade"),
    ),
).drop("ID", "Gender", "Salary")

updateDF2.printSchema()
updateDF2.show(truncate=False)

arrayStructureSchema = StructType(
    [
        StructField(
            "Name",
            StructType(
                [
                    StructField("First_Name", StringType(), True),
                    StructField("Middle_Name", StringType(), True),
                    StructField("Last_Name", StringType(), True),
                ]
            ),
        ),
        StructField("Hobbies", ArrayType(StringType()), True),
        StructField("Properties", MapType(StringType(), StringType()), True),
    ]
)

print(arrayStructureSchema.json())