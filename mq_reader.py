import json

from questdb.ingress import Sender
import os
import sys

import datetime

import pika
from pika.exceptions import StreamLostError


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='ms')

    """def callback(ch, method, properties, body):
        print(f" [x] Received {json.loads(body.decode('utf-8'))}")"""

    channel.basic_consume(queue='ms', on_message_callback=write_to_questdb, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except StreamLostError:
        exit(0)


def write_to_questdb(mech, method, properties, body):
    msg = json.loads(body.decode('utf-8'))
    conf = 'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row("trades",
                   symbols={'symbol': msg.get('symbol'), 'market': msg.get('market').upper(), 'side': msg.get('side')},
                   columns={'price': float(msg.get('price')), 'amount': float(msg.get('amount'))},
                   at= datetime.datetime.fromtimestamp(msg.get('timestamp')))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
