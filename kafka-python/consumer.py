import json, os
from configparser import ConfigParser
from confluent_kafka import Consumer


# Configruation path
if (os.path.dirname(__file__) != ""):
    os.chdir(os.path.dirname(__file__))

# Kafka consumer configurations
config_parser = ConfigParser(interpolation=None)
config_file = open('config.properties', 'r')
config_parser.read_file(config_file)
consumer_config = dict(config_parser['kafka_client'])
consumer_config.update(config_parser['consumer'])

def consume():
    consumer = Consumer(consumer_config)
    consumer.subscribe(['test'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

    consumer.close()


if __name__ == "__main__":
    consume()
