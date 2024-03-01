import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row

#
from pyspark.sql.session import SparkSession

sc = SparkContext("local")
spark = SparkSession(sc)

#
glueContext = GlueContext(SparkContext.getOrCreate())
#
order_list = [
    ["1005", "623", "YES", "1418901234", "75091"],
    ["1006", "547", "NO", "1418901256", "75034"],
    ["1007", "823", "YES", "1418901300", "75023"],
    ["1008", "912", "NO", "1418901400", "82091"],
    ["1009", "321", "YES", "1418902000", "90093"],
]

# Define schema for the order_list
order_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("essential_item", StringType()),
        StructField("timestamp", StringType()),
        StructField("zipcode", StringType()),
    ]
)

# Create a Spark Dataframe from the python list and the schema
df_orders = spark.createDataFrame(order_list, schema=order_schema)
#
df_orders.show()
#
dyf_orders = DynamicFrame.fromDF(df_orders, glueContext, "dyf")
#
dyf_applyMapping = ApplyMapping.apply(
    frame=dyf_orders,
    mappings=[
        ("order_id", "String", "order_id", "Long"),
        ("customer_id", "String", "customer_id", "Long"),
        ("essential_item", "String", "essential_item", "String"),
        ("timestamp", "String", "timestamp", "Long"),
        ("zipcode", "String", "zip", "Long"),
    ],
)
#
dyf_applyMapping.printSchema()
dyf_applyMapping.show()
dyf_orders.show()
