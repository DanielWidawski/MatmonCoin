import json

import pika

def set_mq_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='ms')
    return connection, channel

connection, channel = set_mq_connection()

def write_to_mq(message):
    channel.basic_publish(exchange='', routing_key="ms", body=json.dumps(message))