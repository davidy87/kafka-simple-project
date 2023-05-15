import json, os
from configparser import ConfigParser
from confluent_kafka import Producer


# Configruation path
if (os.path.dirname(__file__) != ""):
    os.chdir(os.path.dirname(__file__))

# Kafka producer configurations
config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
producer_config = dict(config_parser['kafka_client'])

def report(err, msg):
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

def produce():
    producer = Producer(producer_config)
    data_source = ["Hello", "This is for the test"]

    for data in data_source:
        producer.poll(0)
        producer.produce('test', data.encode('utf-8'), callback=report)

    producer.flush()


produce()