import json

from questdb.ingress import Sender
import os
import sys

import datetime

import pika
from pika.exceptions import StreamLostError

from src.db_writer.db_writer import write_to_questdb

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='ms')

def read_from_mq():
    channel.basic_consume(queue='ms', on_message_callback=write_to_questdb, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except StreamLostError:
        print("Stream lost, reconnecting...")
        read_from_mq()

def main():
    read_from_mq()
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
