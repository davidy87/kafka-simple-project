import json, os
from configparser import ConfigParser
from confluent_kafka import Producer
import psycopg2


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

def query_playlist():
    """
    Query all data from Postgres DB and send them to Kafka topic.
    """
    conn = None
    recently_played_songs = []

    try:
        conn = psycopg2.connect(database='my_spotify_playlist', 
                                user='david', 
                                host='localhost',
                                port='5432', 
                                password='testpass123!')
                                
        cursor = conn.cursor()
        query = "SELECT * FROM songs;"
        cursor.execute(query)
        recently_played_songs = cursor.fetchall()

    except (Exception, psycopg2.Error) as err:
        print("Error while fetching data from PostgreSQL:", err)

    finally:
        if conn is not None:
            conn.commit()
            cursor.close()
            conn.close()

        return recently_played_songs

def produce():
    producer = Producer(producer_config)
    data_source = query_playlist()

    for data in data_source:
        producer.poll(0)
        producer.produce('test', str(data).encode('utf-8'), callback=report)

    producer.flush()


if __name__ == "__main__":
    produce()