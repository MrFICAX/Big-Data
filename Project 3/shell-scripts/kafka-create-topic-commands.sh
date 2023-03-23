#! /bin/bash

#docker exec -it kafka-3 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic locations --from-beginning


docker exec -it kafka-3 kafka-topics.sh --create --bootstrap-server kafka-3:9092 --replication-factor 1 --partitions 1 --topic locations

#kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic locations --from-beginning
