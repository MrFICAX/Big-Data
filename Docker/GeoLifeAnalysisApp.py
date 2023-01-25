from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
from pyspark.sql.types import StructType
# from pyspark.sql.functions import split, col
from pyspark.sql.functions import *
import pyspark.sql.functions as F

if __name__ == '__main__':
    # if len(sys.argv) != 2:
    #     print("Usage: main.py <input folder> ")
    #     exit(-1)

    appName = "GeoLifeGpsAnalysis"

    conf = SparkConf()
    #conf.setMaster("spark://spark-master:7077")
    conf.setMaster("local")
    conf.set("spark.driver.memory","4g")

    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()

    geoLifeSchema = StructType() \
                    	.add("time", "string")\
                    	.add("lat", "double")\
                    	.add("lon", "double")\
                        .add("alt", "double")\
                    	.add("label", "string")\
                    	.add("user", "integer")
                    	#.add("", "integer")\
                        #.add("id", "integer")\

   #OVO RADI KADA SE POKRENE LOKALNO (setMaster("local"))
    #booksdata=spark.read.csv("app/geolife_gps_reduced.csv", schema=geoLifeSchema)

    geoLifeDataFrame = spark.read.option("header", True).csv(sys.argv[1], schema=geoLifeSchema)
    #geoLifeDataFrame=spark.read.option("header", True).csv("../geolife_gps_data_example.csv", schema=geoLifeSchema)
    geoLifeDataFrame.show(5)
    geoLifeDataFrame.printSchema()
   
    # df = booksdata.filter(booksdata.user == sys.argv[2])
    # df.show()
    
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
    #df.select('user').distinct().show(2000)

    print(sys.argv[1], sys.argv[2], sys.argv[3])
#OVO RADI
    #df.filter(df['lat'] >= sys.argv[2]).filter(df['lat'] <= sys.argv[3]).show()
#OVO ISTO RADI
    df.filter((df.lat >= sys.argv[2]) & (df.lat <= sys.argv[3])).show()

    df.show(20)

    #print(df.dropDuplicates(["user"]).select("user").collect())
    # filteredDF = df.where(df.lat >= 39).where(df.lat <= 40).select()
    # filteredDF.show()
    # filteredDF.describe()
    # geolifeDF.show()

    # geolifeDF.select("lat", "lon", "alt").describe().show()
        
    spark.stop()
