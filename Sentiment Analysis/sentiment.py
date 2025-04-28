from confulent_kafka import Producer,Consumer, KafkaError
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json
import os
from datetime import datetime


# Kafka Configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'SWJ4E7VFKNXBEOHF',
    'sasl.password': 'gpbsFTK2INYCNIYNNIWYiVldnAeTdpiXZFq4pzsXUK6hjCFaApWat7j5pFqvhY25',
    'group.id': 'sentiment_analysis_group',
    'auto.offset.reset': 'earliest'
}
es = Elasticsearch(
    "https://my-elasticsearch-project-d4dcde.es.us-east-1.aws.elastic.cloud:443",
   api_key = "ckxVNmZaWUJWNWwyVmJ0Q1ZZSE86Q2dyRkdjVl9FU1BlZ3NVMnMwRmpUZw==")

consumer = Consumer(kafka_config)
input_topics = "senti_raw_msg"

class SentimentAnalyzer:
    def __init__(self, kafka_config, es_config):
        self.consumer = Consumer(kafka_config)
        self.es = Elasticsearch(es_config)
        self.producer = Producer(kafka_config)
        self.input_topics = "senti_raw_msg"
        self.output_topic = "senti_final_result"

    def analyze_sentiment(self, text):
        analysis = TextBlob(text)
        return {
            'text': text,
            "polarity": analysis.sentiment.polarity,
            "subjectivity": analysis.sentiment.subjectivity,
            'timestamp': datetime.now().isoformat()
        }
    
    def store_into_elastic(self, sentiment_data):
        es.index(index="sentiment_analysis", document=sentiment_data)

def consume(self):
    self.consumer.subscribe([self.input_topics])
    print("Consumer is running...")

    while True:
        msg = self.consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error in message: {msg.error()}")
            continue
        
        message = json.loads(msg.value().decode('utf-8'))
        text = message.get("text", "")
        
        sentiment_data = self.analyze_sentiment(text)
        
        self.store_into_elastic(sentiment_data)    
            
        print(f"Processed and stored: {sentiment_data}")

        
consumer.subscribe([input_topics])
print("my consumer is runinng ....")

while True :
    msg = consumer.poll(1.0)
    if msg is None :
        continue
    if msg.error():
        print(f"the erro in msg is {msg.error()}")
        continue
    
    message = json.loads(msg.value().decode('utf-8'))
    text = message.get("text","")
    
    
    sentiment_data = analyze_sentiment(text)
    
    store_into_elastic(sentiment_data)    
        
    print(f"all doine till elastic {sentiment_data}")


