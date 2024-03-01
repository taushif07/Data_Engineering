import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())

dataFrame = glueContext.read.csv(
    "/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-4/Day-3/names.csv",
    header=True,
    inferSchema=True,
)
dataFrame.show()

df2 = glueContext.read.csv(
    "/home/taushif/Downloads/Data_Engineering/Data_Engineering_Course_Folder/Month-1/Week-4/Day-3/googleplaystore.csv",
    header=True,
    inferSchema=True,
)

df2.show()

dynamic_df2 = DynamicFrame.fromDF(df2, glueContext, "dyf")

orgs = (
    dynamic_df2.drop_fields(["Android Ver", "Current Ver"])
    .rename_field("Size", "New_Size")
    .rename_field("App", "New_App")
)
orgs.toDF().show()


dynamic_df2.select_fields(["App"]).toDF().distinct().show()

dynamic_df2.select_fields(["App", "Reviews"]).toDF().filter(
    (func.col("Reviews") >= 20000)
).orderBy(func.desc(func.col("Reviews"))).show()
