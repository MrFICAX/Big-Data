import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, window, count, mean, max, min
from pyspark import SparkConf
import sys
from pyspark.ml.feature import StringIndexer, VectorAssembler, MinMaxScaler, StringIndexerModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline

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

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)

    SCALER_LOCATION = os.getenv('SCALER_LOC')
    INDEXER_PIPELINE_LOC = os.getenv('INDEXER_PIPELINE_LOC')
    CLASSIFICATION_MODEL = os.getenv('CLASSIFICATION_MODEL')



    # inputValuesDictionary = readInputValues()
    # print(inputValuesDictionary)

    conf = SparkConf()
    # conf.setMaster("spark://spark-master-x:7077")
    conf.setMaster("local")
    # conf.set("spark.driver.memory","4g")
    # conf.set("spark.cassandra.connection.host", "cassandra")
    # conf.set("spark.cassandra.connection.port", 9042)

    spark = SparkSession.builder.config(
        conf=conf).appName("ModelCreation").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv(sys.argv[1])

    df.show(10)

    label_indexer = StringIndexer(
        inputCol="label", outputCol="label_index", handleInvalid="keep")
    user_indexer = StringIndexer(
        inputCol="user", outputCol="user_index")
    # pipeline = Pipeline(stages=[label_indexer])

    pipeline = Pipeline(stages=[label_indexer, user_indexer])

    fitted_indexer = pipeline.fit(df)

    fitted_indexer.write().overwrite().save(INDEXER_PIPELINE_LOC) #("hdfs://namenode:9000/model/IndexerPipeline")

    indexer_model = PipelineModel.load(INDEXER_PIPELINE_LOC) #("hdfs://namenode:9000/model/IndexerPipeline")

    indexedDf = indexer_model.transform(df)

    # indexedDf = fitted_label_indexer.transform(df)

    indexedDf = indexedDf.drop("label")
    indexedDf = indexedDf.drop("user")

    features = ["lat", "lon", "alt", "user_index",
                "year", "month", "day", "hour", "minute", "second"]

    featureAssembler = VectorAssembler(
        inputCols=features, outputCol="features")

    assembleredDf = featureAssembler.transform(indexedDf)

    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    fitScaler = scaler.fit(assembleredDf)

    fitScaler.write().overwrite().save(SCALER_LOCATION)


    scaledData = fitScaler.transform(assembleredDf)

    scaledData.show()

    finalDf = scaledData.select("scaledFeatures", "label_index")

    # user_indexer = StringIndexer(
    #     inputCol="user", outputCol="user_index").fit(finalDf)

    # finalDfIndexed = user_indexer.transform(finalDf)

    # finishDf = finalDf.select("scaledFeatures", "user_index")

    finalDf.show()

    train_data, test_data = finalDf.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestClassifier(
        featuresCol='scaledFeatures', labelCol='label_index', maxDepth=10)

    rfModel = rf.fit(train_data)

    predictions = rfModel.transform(test_data)


# metricName="f1"
# metricName="precisionByLabel"
# metricName="recallByLabel"

    evaluatorAccuracy = MulticlassClassificationEvaluator(
        labelCol="label_index", predictionCol="prediction", metricName="accuracy")
    evaluatorF1 = MulticlassClassificationEvaluator(
        labelCol="label_index", predictionCol="prediction", metricName="f1")
    evaluatorByLabel = MulticlassClassificationEvaluator(
        labelCol="label_index", predictionCol="prediction", metricName="precisionByLabel")
    evaluatorRecallByLabel = MulticlassClassificationEvaluator(
        labelCol="label_index", predictionCol="prediction", metricName="recallByLabel")

    print(evaluatorAccuracy.evaluate(predictions))
    print(evaluatorAccuracy.evaluate(predictions))
    print(evaluatorByLabel.evaluate(predictions))
    print(evaluatorRecallByLabel.evaluate(predictions))

    rfModel.write().overwrite().save(CLASSIFICATION_MODEL) #("hdfs://namenode:9000/model/RfModel")
