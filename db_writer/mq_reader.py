import time

import pika
from pika.exceptions import StreamLostError

from db_writer import write_to_questdb


def set_mq_connection():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
            channel = connection.channel()
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ...")
            time.sleep(5)


base_connection, base_channel = set_mq_connection()


def read_from_mq(connection=base_connection, channel=base_channel):
    channel.basic_consume(
        queue="ms", on_message_callback=write_to_questdb, auto_ack=True
    )
    print(" [*] Waiting for messages. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except StreamLostError:
        print("Stream lost, reconnecting...")
        new_connection, new_channel = set_mq_connection()
        read_from_mq(new_connection, new_channel)
