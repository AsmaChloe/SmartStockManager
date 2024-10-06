import time
import random
from kafka import KafkaProducer
import json
import pandas as pd

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'temperature-sensor'
data_source_filename = "./data/sales_data.csv"

def load_data(filename=data_source_filename) :
    df = pd.read_csv(filename, sep=',')

def produce_temperature_data():
    sensor_id = random.randint(1, 100)  # Identifiant aléatoire pour un capteur
    temperature = round(random.uniform(15.0, 30.0), 2)  # Température aléatoire
    timestamp = time.time()  # Timestamp actuel
    return {
        'sensor_id': sensor_id,
        'temperature': temperature,
        'timestamp': timestamp
    }

try :
    load_data()

    while True:
        data = produce_temperature_data()
        print(f"Sending data : {data}")

        #Data sent every 30 secs => internal timeout of kafka producer
        result = producer.send(topic, value=data)
        producer.flush()

except KeyboardInterrupt:
    print("Data production has stopped")
finally:
    producer.close()
