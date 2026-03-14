
import sys

from src.filter.kafka_reader import read_from_kafka
from src.filter.mq_writer import write_to_mq
from src.filter.kafka_reader import consumer
from src.filter.mq_writer import connection

from src.filter.market_transform_redirect import market_transformer


def run_pipeline():
    while True:
        try:
            msg = read_from_kafka()
            transformer = market_transformer[msg['market']]
            transformed_msg = transformer(msg)
            write_to_mq(transformed_msg)
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')
        finally:
            consumer.close()
            connection.close()