#! /bin/bash

cd ..

#docker-compose -f docker-compose.yml -f influx-db/docker-compose.yml -f hadoop-db/docker-compose.yml up --build

docker-compose up -d

# cd influx-db
# docker-compose up --build -d

# cd ..

# cd hadoop-db
# docker-compose up --build -d


