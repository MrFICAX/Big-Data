import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, window, count, mean, max, min
from pyspark import SparkConf
from pyspark.sql import Window
import sys

def filterDataFrameByLatitude(df, latMin, latMax):
    print("filterDataFrameByLatitude entered")
    return df.where(col("parsed_values.lat") > latMin).where(col("parsed_values.lat") < latMax)

def filterDataFrameByLongitude(df, lonMin, lonMax):
    print("filterDataFrameByLongitude entered")
    return df.where(col("parsed_values.lon") > lonMin).where(col("parsed_values.lon") < lonMax)

def readInputValues():
    delimiterFound = False
    features = {}
    for arg in sys.argv[1:]:
        if arg == "|":
            delimiterFound = True
            continue
        if not delimiterFound:
            if arg not in features.keys():
                features[arg] = []
        else:
            for key in features.keys():
                if (len(features[key]) != 2):
                    features[key].append(float(arg))
                    break
    return features

def findDfBasedOnInputValues(inputDictionary, inputDf):
        #tmpDf = inputDf.copy()
        tmpDf = inputDf.alias('tmpDf')
        switcher = {
                "lat": filterDataFrameByLatitude,
                "lon": filterDataFrameByLongitude
            }
        for key in inputDictionary.keys():
            method = switcher.get(key, "nothing")
            if (method != "nothing"):
                tmpDf = method(tmpDf, inputDictionary[key][0], inputDictionary[key][1])
                printLine()
                print("Filtered by key: " + key)
        return tmpDf

def printLine():
    print("*******************************************************************************")
    print("-------------------------------------------------------------------------------")
    print("*******************************************************************************")

# org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2, com.datastax.spark:spark-cassandra-connector_2.12:3.0.0

schema = StructType(
    [
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
        StructField("alt", FloatType()),
        StructField("label", StringType()),
        StructField("user", StringType()),
        StructField("year", IntegerType()),
        StructField("month", IntegerType()),
        StructField("day", IntegerType()),
        StructField("hour", IntegerType()),
        StructField("minute", IntegerType()),
        StructField("second", IntegerType()),
        StructField("ts", IntegerType())
    ]
)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    inputValuesDictionary = readInputValues()
    print(inputValuesDictionary)

    
    conf = SparkConf()
    #conf.setMaster("spark://spark-master-x:7077")
    conf.setMaster("local")
    conf.set("spark.driver.memory","4g")
    conf.set("spark.cassandra.connection.host", "cassandra")
    conf.set("spark.cassandra.connection.port", 9042)

    spark = SparkSession.builder.config(conf=conf).appName("AverageLatitude").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "locations")
        .option("startingOffsets", "latest")
        .load()
    )

    # df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

    parsed_valuesWithTimestamp = df.select(
        "timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values")
    )


    parsed_valuesWithTimestamp.printSchema()
    #parsed_valuesWithTimestamp = parsed_valuesWithTimestamp.where(col("parsed_values.lat") > inputValuesDictionary["lat"][0]).where(col("parsed_values.lat") < inputValuesDictionary["lat"][1])

    parsed_valuesWithTimestamp = findDfBasedOnInputValues(inputValuesDictionary, parsed_valuesWithTimestamp)

    timestampXaltitudes = parsed_valuesWithTimestamp.selectExpr("timestamp", "parsed_values.alt AS alt", "parsed_values.user AS user")

    parsed_values = parsed_valuesWithTimestamp.selectExpr("parsed_values.*")
    #parsed_values.printSchema()
    timestamps = parsed_values.selectExpr("ts AS ts")
    latitudes = parsed_values.selectExpr("lat AS lat")
    longitudes = parsed_values.selectExpr( "lon AS lon")
    altitudes = parsed_values.selectExpr("alt AS alt")
    labels = parsed_values.selectExpr("label AS label")
    users = parsed_values.selectExpr("user AS user")


    #timestampXaltitudes.printSchema()
    AltitudeValuesAvgWindows = timestampXaltitudes.groupBy(window(timestampXaltitudes.timestamp, "10 seconds", "5 seconds"), timestampXaltitudes.user).agg(
        mean("alt").alias("meanvalue"),
        max("alt").alias("maxvalue"),
        min("alt").alias("minvalue"),
        count("user").alias("times_data_sent")
    )


    #AltitudeValuesAvgWindows.printSchema()
    AltitudeValuesAvgWindowsForDB = AltitudeValuesAvgWindows.selectExpr("window.start as start",
                                                                        "window.end as end",
                                                                        "user", 
                                                                        "meanvalue", 
                                                                        "maxvalue", 
                                                                        "minvalue", 
                                                                        "times_data_sent" )
    #AltitudeValuesAvgWindowsForDB.printSchema()


    query = (
        AltitudeValuesAvgWindows.writeStream.outputMode("update")
        .queryName("average_latitude")
        .format("console") # format("org.apache.spark.sql.cassandra").options(**load_options)
        .trigger(processingTime="1 seconds")
        .option("truncate", "false")
        .start()
        #.awaitTermination()
    )

    #########################################################################################
    ##################         UPIS U CASSANDRA BAZU PODATAKA         #######################
    #########################################################################################

    def writeToCassandra(writeDF, _):
        writeDF.write \
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="geolocation", keyspace="locations_db")\
            .save()

    AltitudeValuesAvgWindowsForDB.writeStream \
        .option("spark.cassandra.connection.host","cassandra:9042")\
        .foreachBatch(writeToCassandra) \
        .outputMode("update") \
        .start()\
        .awaitTermination()
