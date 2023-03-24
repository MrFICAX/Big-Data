#! /bin/bash

docker container rm StreamingPredictionApp

docker build --rm -t bde/streaming_prediction_app .

docker run --name StreamingPredictionApp --net FT_project3_network -p 4043:4043 bde/streaming_prediction_app

