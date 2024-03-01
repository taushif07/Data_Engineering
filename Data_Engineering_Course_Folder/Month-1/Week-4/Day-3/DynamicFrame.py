from pyspark.context import SparkContext
from pyspark.sql import functions as func
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

# Create GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)


df = glueContext.read.csv(
    "/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-4/Day-3/Airports2.csv",
    header=True,
    inferSchema=True,
)
df.printSchema()
df.show()

dynamic_DF = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

airport_mapped = dynamic_DF.apply_mapping(
    [
        ("Origin_airport", "String", "Destination_airport", "String"),
        ("Passengers", "Integer", "Seats", "Integer"),
        ("Flights", "Integer", "Distance", "Integer"),
    ]
)
airport_mapped.printSchema()
airport_mapped.toDF().show()


airport_with_atleast_1000_seats = (
    dynamic_DF.select_fields(["Origin_airport", "Destination_airport", "Seats"])
    .toDF()
    .filter((func.col("Seats") >= 12000))
    .orderBy(func.desc(func.col("Seats")))
    .limit(10)
)

airport_with_atleast_1000_seats.show()

New_Dynamic_DF = (
    dynamic_DF.rename_field("Origin_airport", "Departure_airport")
    .rename_field("Destination_airport", "Return_airport")
    .select_fields(["Departure_airport", "Return_airport"])
)


New_Dynamic_DF.toDF().show()
