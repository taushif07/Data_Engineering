from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import sum as _sum, desc as _desc, col as _col

spark = SparkSession.builder.master("local").appName("Flight Data Lookup").getOrCreate()

# sqlContext = SQLContext(spark)

df = spark.read.csv("./Airports2.csv", header=True, inferSchema=True)

# df.registerTempTable("df")

df.printSchema()

df.show(50)

print(df.count())  # value is 3606803

df.select(
    "Origin_city",
    "Destination_city",
    "Origin_airport",
    "Destination_airport",
    "Fly_date",
    "Distance",
    "Passengers",
    "Seats",
).show(50)

df.filter(df.Distance >= 200).show()

df.where((df.Origin_airport == "SFO") & (df.Destination_airport == "RDM")).show()

# getting aggregate data

airportAggData = (
    df.groupBy("Origin_city")
    .agg(_sum("Passengers").alias("Total_Passengers"))
    .orderBy(_desc("Total_Passengers"))
)
airportAggData.show()

# Question1 : Find the airport highest no of flight departures

highAirportFlightDeptData = (
    df.groupBy("Origin_airport")
    .agg(_sum("Flights").alias("Total_Flights_Departure"))
    .orderBy(_desc("Total_Flights_Departure"))
)
highAirportFlightDeptData.show()

# Question2 : Find the Airport with Highest Number of Passenger Arrivals

highPassengersArrData = (
    df.groupBy("Destination_airport")
    .agg(_sum("Passengers").alias("Max_Passenger_Arrival"))
    .orderBy(_desc("Max_Passenger_Arrival"))
)
highPassengersArrData.show()


# Question3 : Find the Airport with Most Flight Traffic

originAirportTraffic = (
    df.groupBy("Origin_airport")
    .agg(_sum("Flights").alias("Origin_airport_Traffic"))
    .orderBy(_desc("Origin_airport_Traffic"))
)

destinationAirportTraffic = (
    df.groupBy("Destination_airport")
    .agg(_sum("Flights").alias("Destination_airport_Traffic"))
    .orderBy(_desc("Destination_airport_Traffic"))
)

airportWithMostTraffic = (
    originAirportTraffic.join(
        destinationAirportTraffic,
        originAirportTraffic.Origin_airport
        == destinationAirportTraffic.Destination_airport,
    )
    .select(
        originAirportTraffic.Origin_airport,
        (_col("Origin_airport_Traffic") + _col("Destination_airport_Traffic")).alias(
            "Airport_Flight_Traffic"
        ),
    )
    .orderBy(_desc("Airport_Flight_Traffic"))
)

airportWithMostTraffic.show()

# Question 4 : Find the Airport with Most Passenger Footfall

originAirportPassengerNum = (
    df.groupBy("Origin_airport")
    .agg(_sum("Passengers").alias("Origin_airport_Passenger_Num"))
    .orderBy(_desc("Origin_airport_Passenger_Num"))
)

destinationAirportPassengerNum = (
    df.groupBy("Destination_airport")
    .agg(_sum("Passengers").alias("Destination_airport_Passenger_Num"))
    .orderBy(_desc("Destination_airport_Passenger_Num"))
)

airportWithMostPassengerFootFall = (
    originAirportPassengerNum.join(
        destinationAirportPassengerNum,
        originAirportPassengerNum.Origin_airport
        == destinationAirportPassengerNum.Destination_airport,
    )
    .select(
        originAirportPassengerNum.Origin_airport,
        (
            _col("Origin_airport_Passenger_Num")
            + _col("Destination_airport_Passenger_Num")
        ).alias("Total_Passenger_Footfall"),
    )
    .orderBy(_desc("Total_Passenger_Footfall"))
)

airportWithMostPassengerFootFall.show()
