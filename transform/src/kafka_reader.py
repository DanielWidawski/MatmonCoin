from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers": "kafka:9093",
    "group.id": "my-consumer-group1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

topic = "filter-topic"

consumer = Consumer(consumer_config)

consumer.subscribe([topic])


def check_message_schema(message: dict) -> bool:
    if message.get("market") and message.get("message"):
        # TODO: check this later
        if type(message["message"]) == str:
            message = json.loads(message["message"])
        return message["message"].get("data")
    return False


def read_from_kafka() -> dict:
    msg = consumer.poll(1.0)
    if msg is None:
        print("No message received")
        return None
    if msg.error():
        print(msg.error())
        raise ValueError
    else:
        try:
            new_msg = json.loads(msg.value().decode('utf-8'))
            print(new_msg)
            if check_message_schema(new_msg):
                consumer.commit(msg)
                return new_msg
            else:
                print("Message schema is incorrect")
                consumer.commit(msg)
                return None
        except json.JSONDecodeError:
            print("Failed to decode message")
            consumer.commit(msg)
            return None