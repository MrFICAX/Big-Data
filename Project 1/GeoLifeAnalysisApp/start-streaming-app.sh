#! /bin/bash

docker container rm GeoLifeAnalysisApp

docker build --rm -t bde/spark-app .

docker run --name GeoLifeAnalysisApp --net bde -p 4042:4042 bde/spark-app

