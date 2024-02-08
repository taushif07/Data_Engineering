from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, col, expr, when
from pyspark.sql.types import StructField, StringType, StructType, ArrayType, MapType

spark = SparkSession.builder.master("local[5]").appName("Column_Examples").getOrCreate()

colObj = lit("sparkbyexamples.com")
data = [("James", 23), ("Ann", 40)]
df = spark.createDataFrame(data).toDF("name.fname", "gender")
df.printSchema()

df.select(df.gender).show()
df.select(df["gender"]).show()
df.select(df["`name.fname`"]).show()

df.select(col("gender")).show()

df.select(col("`name.fname`")).show()

data2 = [
    Row(name="James", prop=Row(hair="black", eye="blue")),
    Row(name="Ann", prop=Row(hair="grey", eye="black")),
]

df2 = spark.createDataFrame(data2)
df2.printSchema()

df2.select(df2.prop.hair).show()
df2.select(df2["prop.hair"]).show()
df2.select(col("prop.hair")).show()

df2.select(col("prop.*")).show()


data3 = [(100, 2, 1), (200, 3, 4), (300, 4, 4)]
df3 = spark.createDataFrame(data3).toDF("col1", "col2", "col3")

df3.select(df3.col1 + df3.col2).show()
df3.select(df3.col1 - df3.col2).show()
df3.select(df3.col1 * df3.col2).show()
df3.select(df3.col1 / df3.col2).show()
df3.select(df3.col1 % df3.col2).show()

df3.select(df3.col2 > df3.col3).show()
df3.select(df3.col2 < df3.col3).show()
df3.select(df3.col2 == df3.col3).show()


data4 = [
    ("James", "Bond", "100", None),
    ("Ann", "Varsa", "200", "F"),
    ("Tom Cruise", "XXX", "400", ""),
    ("Tom Brand", None, "400", "M"),
]

columns4 = ["fname", "lname", "id", "gender"]
df4 = spark.createDataFrame(data4, columns4)
df4.show()

df4.select(df4.fname.alias("first_name"), df4.lname.alias("last_name")).show()

df4.select(expr(" fname ||','|| lname").alias("fullName")).show()

# asc, desc to sort
df4.sort(df4.fname.asc()).show()
df4.sort(df4.fname.desc()).show()

# cast
df4.select(df4.fname, df4.id.cast("int")).printSchema()

# between
df4.filter(df4.id.between(100, 300)).show()

# contains
df4.filter(df4.fname.contains("Cruise")).show()

# startswith, endswith()
df4.filter(df4.fname.startswith("T")).show()
df4.filter(df4.fname.endswith("Cruise")).show()

# isNull & isNotNull
df4.filter(df4.lname.isNull()).show()
df4.filter(df4.lname.isNotNull()).show()

# like , rlike
df4.select(df4.fname, df4.lname, df4.id).filter(df4.fname.like("%om"))

# substr
df4.select(df4.fname.substr(1, 2).alias("substr")).show()

# when, otherwise
df4.select(
    df4.fname,
    df4.lname,
    when(df4.gender == "M", "Male")
    .when(df4.gender == "F", "Female")
    .when(df4.gender == None, "")
    .otherwise(df4.gender)
    .alias("new_gender"),
).show()

li = ["100", "200"]

# isin
df4.select(df4.fname, df4.lname, df4.id).filter(df4.id.isin(li)).show()

data5 = [
    (("James", "Bond"), ["Java", "C#"], {"hair": "black", "eye": "brown"}),
    (("Ann", "Varsa"), [".NET", "Python"], {"hair": "brown", "eye": "black"}),
    (("Tom Cruise", ""), ["Python", "Scala"], {"hair": "red", "eye": "grey"}),
    (("Tom Brand", None), ["Perl", "Ruby"], {"hair": "black", "eye": "blue"}),
]

data5Schema = StructType(
    [
        StructField(
            "name",
            StructType(
                [
                    StructField("fname", StringType(), True),
                    StructField("lname", StringType(), True),
                ]
            ),
        ),
        StructField("languages", ArrayType(StringType()), True),
        StructField("properties", MapType(StringType(), StringType()), True),
    ]
)
df5 = spark.createDataFrame(data5, data5Schema)
df5.printSchema()

# getField from MapType
df5.select(df5.properties.getField("hair")).show()

# getField from Struct
df5.select(df5.name.getField("fname")).show()


# getItem() used with ArrayType
df5.select(df5.languages.getItem(1)).show()

# getItem() used with MapType
df5.select(df5.properties.getItem("hair")).show()
