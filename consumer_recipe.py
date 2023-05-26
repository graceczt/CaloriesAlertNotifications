import json
from time import sleep
from kafka import KafkaConsumer


if __name__ == '__main__':
    parsed_topic_name = 'parsed_recipes'
    # Notify user if recipe has more than 200 calories
    calories_threshold = 200

    consumer = KafkaConsumer(parsed_topic_name, group_id='recipe_parser_group', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    

    for msg in consumer:
        record = json.loads(msg.value)
        if 'calories' not in record.keys():
            continue
        calories = int(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            print(f'Alert: {title} calories {calories} exceeds threshold {calories_threshold}')
        else:
            print(f'{title} calories {calories} within threshold {calories_threshold}')
        sleep(3)
    if consumer is not None:
        consumer.close()



