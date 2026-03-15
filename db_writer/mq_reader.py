import pika
from pika.exceptions import StreamLostError

from db_writer import write_to_questdb

def set_mq_connection():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='ms')
    return connection, channel

connection, channel = set_mq_connection()

def read_from_mq():
    channel.basic_consume(
        queue="ms", on_message_callback=write_to_questdb, auto_ack=True
    )
    print(" [*] Waiting for messages. To exit press CTRL+C")
    try:
        channel.start_consuming()
    except StreamLostError:
        print("Stream lost, reconnecting...")
        read_from_mq()
