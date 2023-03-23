#! /bin/bash

docker container rm SparkStreamingApp

docker build --rm -t bde/spark-app .

docker run --name SparkStreamingApp --net project2_default -p 4042:4042 bde/spark-app

