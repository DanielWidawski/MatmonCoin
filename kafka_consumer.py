import json
import sys
from json import JSONDecodeError

from confluent_kafka import Consumer, KafkaException

import pika


def check_message_schema(message: dict) -> bool:
    if message.get("market") and message.get("message"):
        if type(message["message"]) == str:
            message = json.loads(message["message"])
        return message["message"].get("data")
    return False


def get_original_message(message):
    message = message.get('message')
    if type(message.get('message')) == str:
        message = json.loads(message.get('message'))
    return message


def get_original_data(message):
    date = message.get("data")
    if type(date) == str:
        date = json.loads(date)
    return date


def transform(message):
    new_schema = {}
    # message = {'market': 'Binance', 'message': '{"stream":"solusdt@trade","data":{"e":"trade","E":1773087732411,"T":1773087732411,"s":"SOLUSDT","t":3216441787,"p":"85.7800","q":"10.71","X":"MARKET","m":false}}'}
    # message2 = {'market': 'Binance', 'message': {'stream': 'btcusdt@aggTrade', 'data': {'e': 'aggTrade', 'E': 1773136021199, 'a': 3190727436, 's': 'BTCUSDT', 'p': '70974.50', 'q': '0.002', 'nq': '0.002', 'f': 7413816280, 'l': 7413816280, 'T': 1773136021194, 'm': False}}}
    if check_message_schema(message):
        if message.get("market") == 'Binance':
            new_schema['market'] = message.get("market")
            message = get_original_message(message)
            data = get_original_data(message)
            # print(new_schema)
            new_schema['symbol'] = data.get("s")
            new_schema['side'] = "sell" if data.get('m') else "buy"
            new_schema['price'] = data.get("p")
            new_schema['amount'] = data.get("q")
            new_schema['timestamp'] = data.get("T")
        return new_schema
    return None


def set_mq_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='ms')
    return connection, channel


def write_to_mq(channel, message):
    channel.basic_publish(exchange='', body=message)
    #print(" [x] Sent 'Hello World!'")

    
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    
    consumer = Consumer(conf)
    
    topic = 'my-topic'
    consumer.subscribe([topic])
    
    connection, channel = set_mq_connection()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
            else:
                try:
                    new_msg = json.loads(msg.value().decode('utf-8'))
                    transform_ms = transform(new_msg)
                    print(transform_ms)
                    write_to_mq(channel, transform_ms)
                    consumer.commit(msg)
                except JSONDecodeError:
                    consumer.commit(msg)
    
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()
        connection.close()
    
    """
    import json


import sys
from json import JSONDecodeError

from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

consumer = Consumer(conf)

topic = 'my-topic'
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
        else:
            try:
                print(json.loads(msg.value().decode('utf-8')))
            except JSONDecodeError:
                consumer.commit(msg)
            consumer.commit(msg)

except KeyboardInterrupt as e:
    sys.stderr.write('%% Aborted by user\n')
finally:
    consumer.close()
    """
