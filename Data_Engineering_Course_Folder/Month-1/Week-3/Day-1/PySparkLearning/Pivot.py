from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.master("local[5]").appName("Pivot_Examples").getOrCreate()

data = [
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico"),
]

columns = ["Fruits", "Price", "Country"]

df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()

df.show(truncate=False)

pivotDf = df.groupBy(func.col("Fruits")).pivot("Country").agg(func.sum("Price"))

pivotDf.printSchema()

pivotDf.show(truncate=False)

countryColumn = (
    df.select(func.col("Country"))
    .distinct()
    .agg(func.collect_list("Country"))
    .first()[0]
)
print(countryColumn)

pivotDF = (
    df.groupBy(func.col("Fruits"))
    .pivot("Country", countryColumn)
    .agg(func.sum("Price"))
)
pivotDF.show(truncate=False)


unpivotExpr = (
    "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
)
unPivotDF = pivotDF.select("Fruits", func.expr(unpivotExpr)).where("Total is not null")
unPivotDF.show(truncate=False)
