import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, window

spark = SparkSession.builder.appName("AverageLatitude").getOrCreate()

# Get rid of INFO and WARN logs.
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_HOST"])
    .option("subscribe", os.environ["KAFKA_TOPIC"])
    .option("startingOffsets", "latest")
    .option("groupIdPrefix", os.environ["KAFKA_CONSUMER_GROUP"])
    .load()
)

#time,lat,lon,alt,label,user
schema = StructType(
    [
        StructField("time", StringType()),
        StructField("lat", StringType()),
        StructField("lon", StringType()),
        StructField("alt", StringType()),
        StructField("label", StringType()),
        StructField("user", StringType()),
    ]
)

# Parse the "value" field as JSON format.
parsed_values = df.select(
    "timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values")
)
# We only need the Age column for now.
latitudes = parsed_values.selectExpr("timestamp", "parsed_values.lat AS lat")

dates = parsed_values.selectExpr("parsed_values.time AS time")
latitudes = parsed_values.selectExpr("parsed_values.lat AS lat")
longitudes = parsed_values.selectExpr( "parsed_values.lon AS lon")
altitudes = parsed_values.selectExpr("parsed_values.alt AS alt")
labels = parsed_values.selectExpr("parsed_values.label AS label")
users = parsed_values.selectExpr("parsed_values.user AS user")


# We set a window size of 10 seconds, sliding every 5 seconds.
# averageAge = latitudes.groupBy(window(latitudes.timestamp, "10 seconds", "5 seconds")).agg(
#     {"lat": "avg"}
# )

query = (
    latitudes.writeStream.outputMode("update")
    .queryName("average_latitude")
    .format("console")
    .trigger(processingTime="10 seconds")
    .option("truncate", "false")
    .start()
    .awaitTermination()
)
