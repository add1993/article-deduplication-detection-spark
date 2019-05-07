from kafka import KafkaProducer
from newsplease import NewsPlease
from time import sleep
import json, sys
import requests
import time
import os
import re

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
    
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__== "__main__":
    basePath = './data/nytimes.com'
    prod = connect_kafka_producer();
    for item in os.listdir(basePath):
        if '.json' in item:
            continue

        filePath = basePath + '/' + item
        print(filePath)
        text = open(filePath, 'r', encoding='utf-8').read()
        article = NewsPlease.from_html(text, url=None)
        print(article.text)
        story = article.text
        publish_message(prod, 'test', story)
        time.sleep(1)
        
    if prod is not None:
        prod.close()
            
