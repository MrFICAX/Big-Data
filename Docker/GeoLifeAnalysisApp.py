from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
from pyspark.sql.types import StructType
# from pyspark.sql.functions import split, col
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import copy
from pyspark.sql.types import IntegerType


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
    for arg in sys.argv[2:]:
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
    # if len(sys.argv) != 2:
    #     print("Usage: main.py <input folder> ")
    #     exit(-1)

    appName = "GeoLifeGpsAnalysis"

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    #conf.setMaster("local")
    conf.set("spark.driver.memory","4g")

    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()

    geoLifeSchema = StructType() \
                    	.add("time", "string")\
                    	.add("lat", "float")\
                    	.add("lon", "float")\
                        .add("alt", "float")\
                    	.add("label", "string")\
                    	.add("user", "integer")
                    	#.add("", "integer")\
                        #.add("id", "integer")\

   #OVO RADI KADA SE POKRENE LOKALNO (setMaster("local"))
    #booksdata=spark.read.csv("app/geolife_gps_reduced.csv", schema=geoLifeSchema)

    geoLifeDataFrame = spark.read.option("header", True).csv(sys.argv[1], schema=geoLifeSchema)
                    #geoLifeDataFrame=spark.read.option("header", True).csv("../geolife_gps_data_example.csv", schema=geoLifeSchema)
    # geoLifeDataFrame.show(5)
    # geoLifeDataFrame.printSchema()
       
    splitDFTime = geoLifeDataFrame.withColumn("date",split(col("time")," ").getItem(0))\
        .withColumn("exacttime",split(col("time")," ").getItem(1))\
        .drop("time")
    # splitDFTime.printSchema()
    # splitDFTime.show(10)

    splitDFYear = splitDFTime.withColumn("year",split(col("date"),"-").getItem(0))\
        .withColumn("month",split(col("date"),"-").getItem(1))\
        .withColumn("day",split(col("date"),"-").getItem(2))\
        .drop("date")

    # splitDFYear.printSchema()
    # splitDFYear.show()

    splitDFTime = splitDFYear.withColumn("hour",split(col("exacttime"),":").getItem(0))\
        .withColumn("minute",split(col("exacttime"),":").getItem(1))\
        .withColumn("second",split(col("exacttime"),":").getItem(2))\
        .drop("exacttime")
    # splitDFTime.printSchema()
    # splitDFTime.show()


    df = splitDFTime.withColumn('hour', F.regexp_replace('hour', r'^[0]*', ''))
    df = df.withColumn('minute', F.regexp_replace('minute', r'^[0]*', ''))
    df = df.withColumn('second', F.regexp_replace('second', r'^[0]*', ''))
    df = df.withColumn('day', F.regexp_replace('day', r'^[0]*', ''))
    df = df.withColumn('month', F.regexp_replace('month', r'^[0]*', ''))

 
    df.show(20)
    df.printSchema()

    # df= df.withColumn("year",col("year").cast(IntegerType))\
    #         .withColumn("month",col("month").cast(IntegerType))\
    #          .withColumn("day",col("day").cast(IntegerType))\
    #           .withColumn("hour",col("hour").cast(IntegerType))\
    #            .withColumn("minute",col("minute").cast(IntegerType))\
    #             .withColumn("second",col("second").cast(IntegerType))

    cols = ["year", "month", "day", "hour", "minute", "second"]
    for col_name in cols:
        df = df.withColumn(col_name, col(col_name).cast('float'))

    df.show(20)
    df.printSchema() 
    

    inputValuesDictionary = readInputValues()
    print(inputValuesDictionary)

    filteredDf = findDfBasedOnInputValues(inputValuesDictionary, df)
    filteredDf.printSchema()
    filteredDf.show(25)
    filteredDf.describe().show()

    #SHOWING ALL USERS AT EXACT PARAMETER VALUES
    df.select('user').distinct().show(2000)

    df.select('label').distinct().show(2000)


                

#OVO RADI
    #df.filter(df['lat'] >= sys.argv[2]).filter(df['lat'] <= sys.argv[3]).show()
#OVO ISTO RADI
    #df.filter((df.lat >= sys.argv[2]) & (df.lat <= sys.argv[3])).show()





    #df.show(20)

    #print(df.dropDuplicates(["user"]).select("user").collect())
    # filteredDF = df.where(df.lat >= 39).where(df.lat <= 40).select()
    # filteredDF.show()
    # filteredDF.describe()
    # geolifeDF.show()

    # geolifeDF.select("lat", "lon", "alt").describe().show()
        
    spark.stop()
