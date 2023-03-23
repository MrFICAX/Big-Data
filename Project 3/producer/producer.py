import time
import os
import csv
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[os.environ["KAFKA_HOST"]],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 11),
)

while True:
    with open(os.environ["DATA"], "r") as file:
        reader = csv.reader(file, delimiter=",")
        headers = next(reader)
        for row in reader:
            value = {headers[i]: row[i] for i in range(len(headers))}
            value["lat"] = float(value["lat"])
            value["lon"] = float(value["lon"])
            value["alt"] = float(value["alt"])
            value["user"] = int(value["user"])
            value["ts"] = int(time.time())
            value["year"] = int(value["year"])
            value["month"] = int(value["month"])
            value["day"] = int(value["day"])
            value["hour"] = int(value["hour"])
            value["minute"] = int(value["minute"])
            value["second"] = int(value["second"])
            print(value)
            producer.send(os.environ["KAFKA_TOPIC"], value=value)
            time.sleep(float(os.environ["KAFKA_INTERVAL"]))
