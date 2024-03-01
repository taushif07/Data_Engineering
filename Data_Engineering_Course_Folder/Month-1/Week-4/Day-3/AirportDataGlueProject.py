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

# Question1 : Find the airport highest no of flight departures

highestNumOfFlightDepartures = (
    dynamic_DF.toDF()
    .groupBy(func.col("Origin_airport").alias("Airport"))
    .agg(func.sum(func.col("Flights")).alias("Number_Of_Flights"))
    .orderBy(func.desc("Number_Of_Flights"))
)

highestNumOfFlightDepartures.show(10, truncate=False)

# Question2 : Find the Airport with Highest Number of Passenger Arrival

highestNoOfPassengersArrival = (
    dynamic_DF.toDF()
    .groupBy(func.col("Origin_airport").alias("Airport"))
    .agg(func.sum(func.col("Passengers")).alias("Total_Passengers_Arrived"))
    .orderBy(func.desc("Total_Passengers_Arrived"))
)
highestNoOfPassengersArrival.show(10, truncate=False)

# Question3 : Find the Airport with Most Flight Traffic

origin_airport_traffic = (
    dynamic_DF.toDF()
    .groupBy(func.col("Origin_airport"))
    .agg(func.sum(func.col("Flights")).alias("Origin_airport_Traffic"))
    .orderBy(func.desc("Origin_airport_Traffic"))
)

destination_airport_traffic = (
    dynamic_DF.toDF()
    .groupBy(func.col("Destination_airport"))
    .agg(func.sum(func.col("Flights")).alias("Destination_airport_traffic"))
    .orderBy(func.desc("Destination_airport_traffic"))
)

AirportWithMostTraffic = (
    origin_airport_traffic.join(
        destination_airport_traffic,
        destination_airport_traffic["Destination_airport"]
        == origin_airport_traffic["Origin_airport"],
        "inner",
    )
    .select(
        func.col("Origin_airport").alias("Airport"),
        (
            func.col("Origin_airport_Traffic") + func.col("Destination_airport_traffic")
        ).alias("Total_Airport_Traffic"),
    )
    .orderBy(func.desc("Total_Airport_Traffic"))
)

AirportWithMostTraffic.show(10, truncate=False)
