#! /bin/bash

docker container rm ModelCreationApp

docker build --rm -t bde/model_creation_app .

docker run --name ModelCreationApp --net FT_project3_network -p 4042:4042 bde/model_creation_app

