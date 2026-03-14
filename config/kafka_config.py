producer_config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'localhost:9092',

    # Fixed properties
    'acks': 'all'
}

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "my-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

topic = "test"