import json
import sys

from confluent_kafka import Consumer, KafkaException

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
            print(json.loads(msg.value().decode('utf-8')))
            consumer.commit(msg)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    consumer.close()
