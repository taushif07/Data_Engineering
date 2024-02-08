from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName("FlatMap_Examples").getOrCreate()

data = [
    "Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s",
]

rdd = spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)


rdd2 = rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)


arrayData = [
    ("James", ["Java", "Scala"], {"hair": "black", "eye": "brown"}),
    ("Michael", ["Spark", "Java", None], {"hair": "brown", "eye": None}),
    ("Robert", ["CSharp", ""], {"hair": "red", "eye": ""}),
    ("Washington", None, None),
    ("Jefferson", ["1", "2"], {}),
]

df = spark.createDataFrame(
    data=arrayData, schema=["name", "knownLanguages", "properties"]
)

df2 = df.select(df.name, explode(df.knownLanguages).alias("Language"))
df2.printSchema()
df2.show()
