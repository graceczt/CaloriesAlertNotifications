import requests
import json
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from time import sleep
import re
import uuid

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer
    
def get_recipe():
    recipes = []

    url = 'https://www.allrecipes.com/recipes/1540/fruits-and-vegetables/vegetables/potatoes/'
    print(f'Fetching {url}')

    try:
        r = requests.get(url)
        if r.status_code == 200:
            print(f'Fetched {url}')
            url = r.text
            soup = BeautifulSoup(url, 'html.parser')
            regex = re.compile('mntl-card-list-items_*')
            links = soup.find_all('a', {'id': regex})
            # Get all the recipe links from url
            for link in links:
                recipes.append(link.get('href'))

    except Exception as e:
        print(f'Error fetching {url}: {e}')

    finally:
        return recipes
    
    
if __name__ == '__main__':
    recipes = get_recipe()
    if recipes is not None:
        print(f'Got {len(recipes)} recipes')
        kafka_producer = connect_kafka_producer()
        for recipe in recipes:
            _uuid = str(uuid.uuid4())
            publish_message(kafka_producer, 'recipes', _uuid, recipe)
        if kafka_producer is not None:
            kafka_producer.close()
    

# run the producer by calling python producer_recipe.py
# check the topic by calling kafka-console-consumer --bootstrap-server localhost:9092 --topic recipes --from-beginning



    

