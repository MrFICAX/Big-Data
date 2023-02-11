import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, window
from pyspark import SparkConf

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0, org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

conf = SparkConf()
conf.setMaster("spark://spark-master-x:7077")   #("spark://spark-master-x:7077")
#conf.setMaster("local")
conf.set("spark.driver.memory","4g")


spark = SparkSession.builder.config(conf=conf).appName("AverageLatitude").getOrCreate()

# Get rid of INFO and WARN logs.
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "locations")
    .option("startingOffsets", "latest")
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
