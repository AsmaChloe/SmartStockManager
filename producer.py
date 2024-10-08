import time
import random
from kafka import KafkaProducer
import json
import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'order-sensor'

def generate_order_id():
    #TODO
    return 0

def generate_order_data(customer_id):
    order_id = generate_order_id() 
    order_status_id = 0
    customer_id = customer_id
    order_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return {
        "order_id" : order_id,
        "order_status_id" : order_status_id,
        "customer_id" : customer_id,
        "order_date" : order_date
    }

def pick_product() :
    #TODO
    product_data = {
        "product_id" : 0,
        "product_name" : "USB Cable",
        "category_id" : 0,
        "price" : 0.99,
        "min_stock_threshold" : 10
    }
    return product_data

def generate_product_order_id() :
    #TODO
    return 0

def generate_product_order(customer_id) :

    order_data = generate_order_data(customer_id)
    order_id = order_data['order_id']

    nb_product = random.randint(1,6)

    product_order_data_list = []
    for i in range(nb_product) :
        product_data = pick_product()    
        product_id = product_data['product_id']

        quantity = random.randint(1,6) #TODO vérifier la limite des stocks

        product_order_data = {
            "product_order_id" : generate_product_order_id(),
            "product_id" : product_id,
            "order_id" : order_id,
            "quantity" : quantity
        }

        product_order_data_list.append(product_order_data)
    #TODO save product_order_data

    return order_data

try :

    while True:
        data = generate_product_order(0)

        print(f"Sending data : {data}")

        #Data sent every 30 secs => internal timeout of kafka producer
        result = producer.send(topic, value=data)
        producer.flush()

except KeyboardInterrupt:
    print("Data production has stopped")
finally:
    producer.close()
