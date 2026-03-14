from confluent_kafka import Consumer
from config.kafka_config import consumer_config, topic
import json


consumer = Consumer(consumer_config)

consumer.subscribe([topic])


def check_message_schema(self, message: dict) -> bool:
    if message.get("market") and message.get("message"):
        # TODO: check this later
        if type(message["message"]) == str:
            message = json.loads(message["message"])
        return message["message"].get("data")
    return False


def read_from_kafka() -> dict:
    msg = consumer.poll(1.0)
    if msg is None:
        raise TypeError
    if msg.error():
        print(msg.error())
        raise ValueError
    else:
        try:
            dict_msg = json.loads(msg.value().decode("utf-8"))
            if check_message_schema(msg):
                return dict_msg
            else:
                raise AttributeError
        except json.JSONDecodeError:
            consumer.commit(msg)
    consumer.commit(msg)
