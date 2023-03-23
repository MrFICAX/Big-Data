#! /bin/bash


docker exec -it kafka-3 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic locations --from-beginning