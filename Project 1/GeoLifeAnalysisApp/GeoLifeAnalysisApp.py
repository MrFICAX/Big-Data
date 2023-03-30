from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
from pyspark.sql.types import StructType
# from pyspark.sql.functions import split, col
from pyspark.sql.functions import *
import pyspark.sql.functions as F


def filterDataFrameByLatitude(df, latMin, latMax):
    print("filterDataFrameByLatitude entered")
    return df.filter((df.lat >= latMin) & (df.lat <= latMax))

def filterDataFrameByLongitude(df, lonMin, lonMax):
    print("filterDataFrameByLongitude entered")
    return df.filter((df.lon >= lonMin) & (df.lon <= lonMax))

def filterDataFrameByAltitude(df, altMin, altMax):
    return df.filter((df.alt >= altMin) & (df.alt <= altMax))    

def filterDataFrameByYear(df, yearMin, yearMax):
    print("filterDataFrameByYear entered")
    return df.filter((df.year >= yearMin) & (df.year <= yearMax))
    
def filterDataFrameByMonth(df, monthMin, monthMax):
    return df.filter((df.month >= monthMin) & (df.month <= monthMax))

def filterDataFrameByDay(df, dayMin, dayMax):
    return df.filter((df.day >= dayMin) & (df.day <= dayMax))

def filterDataFrameByHour(df, hourMin, hourMax):
    return df.filter((df.hour >= hourMin) & (df.hour <= hourMax))
    
def filterDataFrameByMinute(df, minuteMin, minuteMax):
    return df.filter((df.minute >= minuteMin) & (df.minute <= minuteMax))

def filterDataFrameBySecond(df, secondMin, secondMax):
    return df.filter((df.second >= secondMin) & (df.second <= secondMax))

def readInputValues():
    delimiterFound = False
    features = {}
    for arg in sys.argv[3:]:
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
        tmpDf.show(20)
        switcher = {
                "lat": filterDataFrameByLatitude,
                "lon": filterDataFrameByLongitude,
                "alt": filterDataFrameByAltitude,
                "year": filterDataFrameByYear,
                "month": filterDataFrameByMonth,
                "day": filterDataFrameByDay,
                "hour": filterDataFrameByHour,
                "minute": filterDataFrameByMinute,
                "second": filterDataFrameBySecond,
            }
        for key in inputDictionary.keys():
            method = switcher.get(key, "nothing")
            if (method != "nothing"):
                tmpDf = method(tmpDf, inputDictionary[key][0], inputDictionary[key][1])
                printLine()
                print("Filtered by key: " + key)
                tmpDf.show(20)
        return tmpDf

def printLine():
    print("*******************************************************************************")
    print("-------------------------------------------------------------------------------")
    print("*******************************************************************************")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    appName = "GeoLifeGpsAnalysis"

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    #conf.setMaster("local")
    #conf.set("spark.driver.memory","4g")

    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    geoLifeSchema = StructType() \
                    	.add("time", "string")\
                    	.add("lat", "float")\
                    	.add("lon", "float")\
                        .add("alt", "float")\
                    	.add("label", "string")\
                    	.add("user", "integer")

    geoLifeDataFrame = spark.read.option("header", True).csv(sys.argv[1], schema=geoLifeSchema)
                    #geoLifeDataFrame=spark.read.option("header", True).csv("../geolife_gps_data_example.csv", schema=geoLifeSchema)
    geoLifeDataFrame.show(25)
    #geoLifeDataFrame.printSchema()
       
    splitDFTime = geoLifeDataFrame.withColumn("date",split(col("time")," ").getItem(0))\
        .withColumn("exacttime",split(col("time")," ").getItem(1))\
        .drop("time")

    splitDFYear = splitDFTime.withColumn("year",split(col("date"),"-").getItem(0))\
        .withColumn("month",split(col("date"),"-").getItem(1))\
        .withColumn("day",split(col("date"),"-").getItem(2))\
        .drop("date")


    splitDFTime = splitDFYear.withColumn("hour",split(col("exacttime"),":").getItem(0))\
        .withColumn("minute",split(col("exacttime"),":").getItem(1))\
        .withColumn("second",split(col("exacttime"),":").getItem(2))\
        .drop("exacttime")

    df = splitDFTime.withColumn('hour', F.regexp_replace('hour', r'^[0]*', ''))
    df = df.withColumn('minute', F.regexp_replace('minute', r'^[0]*', ''))
    df = df.withColumn('second', F.regexp_replace('second', r'^[0]*', ''))
    df = df.withColumn('day', F.regexp_replace('day', r'^[0]*', ''))
    df = df.withColumn('month', F.regexp_replace('month', r'^[0]*', ''))

    cols = ["year", "month", "day", "hour", "minute", "second"]
    for col_name in cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))

    df.show(20)
    df.printSchema() 
    

    inputValuesDictionary = readInputValues()
    print(inputValuesDictionary)

    filteredDf = findDfBasedOnInputValues(inputValuesDictionary, df)
    print("FILTERED DATA:")
    filteredDf.printSchema()
    filteredDf.show(25)
    filteredDf.describe().show()

    filteredDf.coalesce(1).write.mode('overwrite').option("header",True) \
     .csv(sys.argv[2])

    print("SELECT USERS WHICH WALKED FROM FILTERED DATA:")
    filteredDf.filter(df.label == "walk").select('user').distinct().sort('user').show(truncate=False)

    print("MEAN VALUE OF LATITUDE, LONGITUDE AND ALTITUDE IN FILTERED DATAFRAME:")
    filteredDf.select(mean('lat'), mean('lon'), mean('alt')).show()

    print("MIN VALUE OF LATITUDE, LONGITUDE AND ALTITUDE IN FILTERED DATAFRAME:")
    filteredDf.select(min('lat'), min('lon'), min('alt')).show()

    print("MAX VALUE OF LATITUDE, LONGITUDE AND ALTITUDE IN FILTERED DATAFRAME:")
    filteredDf.select(max('lat'), max('lon'), max('alt')).show()

    print("DISTINCT USER VALUES IN FILTERED DATAFRAME:")
    filteredDf.select('user').distinct().sort('user').show(2000)

    print("DISTINCT LABEL VALUES IN FILTERED DATAFRAME:")
    filteredDf.select('label').distinct().show(2000)

    spark.stop()
