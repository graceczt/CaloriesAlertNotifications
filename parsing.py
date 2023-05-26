import json
import re

from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
import requests

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
    
def parse(recipeurl):
    title = '_'
    author = '_'
    description = '_'
    calories = 0
    ingredients = []
    recorddict = {}

    # if has /recipe/ in url, then it is a recipe
    # then return None if not recipe
    # 8536677 is recipe id, save it in id field
    # https://www.allrecipes.com/recipe/8536677/copycat-kfc-mashed-potatoes/
    if '/recipe/' not in str(recipeurl):
        return None
    
    recorddict['id'] = str(recipeurl).split('/')[4]

    # get the markup by request by recipeurl
    markup = requests.get(recipeurl).text

    soup = BeautifulSoup(markup, 'html.parser')

    # title 
    try:
        titleregex = re.compile('article-heading_*')
        title = soup.find('h1', {'class': titleregex}).text.strip()
    except:
        title = ''
    finally:
        recorddict['title'] = title

    # author
    try:
        authorregex = re.compile('mntl-attribution__item-name')
        author = soup.find('a', {'class': authorregex}).text.strip()
    except:
        author = soup.find('span', {'class': authorregex}).text.strip()
    finally:
        recorddict['author'] = author

    # description
    try:
        descriptionregex = re.compile('article-subheading_*')
        description = soup.find('p', {'class': descriptionregex}).text.strip()
    except:
        description = ''
    finally:
        recorddict['description'] = description

    # calories
    try:
        caloriesregex = re.compile('mntl-nutrition-facts-summary__*')
        calories = soup.find('td', {'class': caloriesregex}).text.strip()
    except:
        calories = 0
    finally:
        recorddict['calories'] = calories

    # ingredients
    try:
        ingredientsregex = re.compile('mntl-structured-ingredients__list-item')
        ingredientsLI = soup.find_all('li', {'class': ingredientsregex})
        ingredients = []
        for li in ingredientsLI:
            spans = li.find_all('span')
            ingredient = ' '.join([span.text.strip() for span in spans])
            ingredients.append(ingredient)
    except:
        ingredients = []
    finally:
        recorddict['ingredients'] = ingredients

    return recorddict
    

if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'recipes'
    parsed_topic_name = 'parsed_recipes'

    consumer = KafkaConsumer(topic_name, group_id='recipe_parser_group', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    producer = connect_kafka_producer()
    
    for i, msg in enumerate(consumer):
        if msg.value is None:
            continue
        import time
        time.sleep(5)
        print('Loading {}'.format(i+1))
        recipe = parse(msg.value)
        if recipe is None:
            continue
        print('Processed {}'.format(recipe['id']))

        recipeJson = json.dumps(recipe)
        publish_message(producer, parsed_topic_name, recipe['id'], recipeJson)

    consumer.close()
    producer.close()
