import sys
from kafka_reader import read_from_kafka
from mq_writer import write_to_mq
from kafka_reader import consumer
from mq_writer import connection
from market_transform_redirect import market_transformer


def run_pipeline():
    while True:
        try:
            msg = read_from_kafka()
            transformer = market_transformer[msg["market"].upper()]
            transformed_msg = transformer(msg)
            write_to_mq(transformed_msg)
        except KeyboardInterrupt:
            sys.stderr.write("%% Aborted by user\n")
        except TypeError:
            sys.stderr.write("%% None message encountered\n")
        finally:
            consumer.close()
            connection.close()


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
