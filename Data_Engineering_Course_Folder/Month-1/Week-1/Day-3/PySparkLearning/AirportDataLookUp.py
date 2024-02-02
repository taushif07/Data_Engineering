from pyspark.sql import SparkSession, functions as func

spark = (
    SparkSession.builder.master("local")
    .appName("Airport_Data_Question_Practice")
    .getOrCreate()
)

df = spark.read.csv("./Airports2.csv", header=True, inferSchema=True)
df.printSchema()
df.show(50)

# Question 5 : Find the Occupancy Rate for Most Popular Routes

flightData = (
    df.groupBy(
        func.least(func.col("Origin_airport"), func.col("Destination_airport")).alias(
            "Airport_1"
        ),
        func.greatest(
            func.col("Destination_airport"), func.col("Origin_airport")
        ).alias("Airport_2"),
    )
    .agg(
        func.sum("Flights").alias("Flights"),
        func.sum("Passengers").alias("Passengers"),
        func.sum("Seats").alias("Seats"),
    )
    .orderBy(func.desc("Airport_1"), func.desc("Airport_2"))
)

occupancyRate = (
    flightData.withColumn(
        "Occupancy_Rate", (func.col("Passengers") * 100 / func.col("Seats"))
    )
    .filter(
        (func.col("Occupancy_Rate").isNotNull()) & (func.col("Occupancy_Rate") <= 100)
    )
    .orderBy(
        func.desc("Flights"),
        func.desc("Passengers"),
        func.desc("Seats"),
        func.desc("Occupancy_Rate"),
    )
    .limit(20)
)

occupancyRate.show()

# Question 6: Find the Number of Flights for Long Distance Journeys

numOfFlightLongDistJourney = (
    df.withColumn(
        "Airport_1",
        func.least(func.col("Origin_airport"), func.col("Destination_airport")),
    )
    .withColumn(
        "Airport_2",
        func.greatest(func.col("Origin_airport"), func.col("Destination_airport")),
    )
    .groupBy("Airport_1", "Airport_2")
    .agg(
        func.mean(func.col("Distance")).alias("Distance"),
        func.sum(func.col("Flights")).alias("Flights"),
    )
    .filter(func.col("Flights") > 0)
    .orderBy(func.desc("Distance"))
    .limit(20)
)

numOfFlightLongDistJourney.show()

# Question 7 : Find the Average Distances for Routes with Most Flights

avgDistForMostFlights = (
    df.withColumn(
        "Airport_1",
        func.least(func.col("Origin_airport"), func.col("Destination_airport")),
    )
    .withColumn(
        "Airport_2",
        func.greatest(func.col("Origin_airport"), func.col("Destination_airport")),
    )
    .groupBy("Airport_1", "Airport_2")
    .agg(
        func.avg(func.col("Distance")).alias("Distance"),
        func.sum(func.col("Flights")).alias("Flights"),
    )
    .filter(func.col("Flights") > 0)
    .orderBy(func.desc("Flights"))
    .limit(20)
)

avgDistForMostFlights.show()
