#! /bin/bash

cd ..

#docker-compose -f docker-compose.yml -f influx-db/docker-compose.yml -f hadoop-db/docker-compose.yml up --build

docker-compose up --build -d