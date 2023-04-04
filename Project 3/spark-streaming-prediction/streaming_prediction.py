import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, window, count, mean, max, min
from pyspark import SparkConf
import sys
from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScalerModel
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline

# InfluxDB libraries
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

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
        StructField("second", IntegerType())
    ]
)

INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
INFLUXDB_PORT = os.getenv('INFLUXDB_PORT')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')

class InfluxDBWriter:
    def __init__(self):
        self._org = INFLUXDB_ORG
        self._token = INFLUXDB_TOKEN
        self.client = InfluxDBClient(url=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}", token=self._token, org=self._org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print(f"Opened {partition_id}, {epoch_id}")
        return True

    def process(self, row):
        self.write_api.write(bucket=INFLUXDB_BUCKET, record=self._row_to_point(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print(f"Closed with error: {error}")

    def _row_to_point(self, row):
        print(row)

        # timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        point = (
            Point.measurement("locations_measurement2")
            .tag("measure", "locations_measurement2")
            .time(datetime.utcnow(), WritePrecision.NS)
            .field("Latitude", float(row['lat']))
            .field("Longitude", float(row['lon']))
            .field("Altitude", float(row['alt']))
            .field("Label_index", int(row['label_index']))
            .field("User_index", int(row['user_index']))
            .field("Label_prediction", int(row['prediction']))
        )
        print("Label_index", int(row['label_index']), "Label_prediction", int(row['prediction']))
        print()
        return point

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    INDEXER_PIPELINE_LOC = os.getenv('INDEXER_PIPELINE_LOC')
    SCALER_LOCATION = os.getenv('SCALER_LOC')
    CLASSIFICATION_MODEL = os.getenv('CLASSIFICATION_MODEL')

    INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
    INFLUXDB_PORT = os.getenv('INFLUXDB_PORT')
    INFLUXDB_USERNAME = os.getenv('INFLUXDB_USERNAME')
    INFLUXDB_PASSWORD = os.getenv('INFLUXDB_PASSWORD')
    INFLUXDB_DATABASE = os.getenv('INFLUXDB_DATABASE')

    KAFKA_HOST = os.getenv('KAFKA_HOST')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
    KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP')


    conf = SparkConf()
    conf.setMaster("spark://spark-master-3:7077")
    #conf.setMaster("local")
    # conf.set("spark.driver.memory","4g")
    # conf.set("spark.cassandra.connection.host", "cassandra")
    # conf.set("spark.cassandra.connection.port", 9042)

    spark = SparkSession.builder.config(
        conf=conf).appName("StreamingPreditionApp").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_HOST)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("groupIdPrefix", KAFKA_CONSUMER_GROUP)
        .load()
    )

    loaded_values = df.select(
        from_json(col("value").cast(
            "string"), schema).alias("data")
    )

    parsed_values = loaded_values.selectExpr("data.*")
    parsed_values.printSchema()

    indexer_model = PipelineModel.load(INDEXER_PIPELINE_LOC) #("hdfs://namenode:9000/model/IndexerPipeline")
    scaler_model = MinMaxScalerModel.load(SCALER_LOCATION)
    classification_model = RandomForestClassificationModel.load(CLASSIFICATION_MODEL)


    indexedDf = indexer_model.transform(parsed_values)

    indexedDf = indexedDf.drop("label")
    indexedDf = indexedDf.drop("user")

    features = ["lat", "lon", "alt", "user_index",
                "year", "month", "day", "hour", "minute", "second"]

    featureAssembler = VectorAssembler(
        inputCols=features, outputCol="features")

    assembleredDf = featureAssembler.transform(indexedDf)

    finalData = scaler_model.transform(assembleredDf)

    prediction = classification_model.transform(finalData)

    prediction.printSchema()

    query = prediction.writeStream \
        .foreach(InfluxDBWriter()) \
        .start()
    query.awaitTermination()

    # evaluatorAccuracy = MulticlassClassificationEvaluator(
    #     labelCol="user_index", predictionCol="prediction", metricName="accuracy")
    
    #prediction = prediction.withColumn('my_prediction', evaluatorAccuracy.evaluate(prediction))

    # prediction = prediction.drop('rawPrediction')
    # prediction = prediction.drop('probability')

    # prediction = prediction.drop('scaledFeatures')

    # query = (
    #     prediction.writeStream.outputMode("update")
    #     .queryName("average_latitude")
    #     .format("console") # format("org.apache.spark.sql.cassandra").options(**load_options)
    #     .trigger(processingTime="0 seconds")
    #     .option("truncate", "false")
    #     .start()
    #     .awaitTermination()
    # )


    # print(f"> Stampanje u konzoli ...")
    # query = (prediction
    #         #.withWatermark("timestamp", "1 minute")
    #         .writeStream
    #         .outputMode("update")
    #         .queryName("DeesriptiveAnalysis")
    #         .format("console")
    #         .trigger(processingTime="5 seconds")
    #         .option("truncate", "false")
    #         #.foreachBatch(writeToCassandra)
    #         .start()
    # )


    #print(evaluatorAccuracy.evaluate(prediction))
    
