from src.kafka_reader import read_from_kafka
from src.mq_writer import write_to_mq
from src.kafka_reader import consumer
from src.mq_writer import connection
from src.market_transform_redirect import market_transformer


def run_pipeline():
    try:
        while True:
            try:
                message = read_from_kafka()
                if message is not None:
                    market = message.get("market").upper()
                    if market in market_transformer:
                        transformed_message = market_transformer[market].transform(message)
                        write_to_mq(transformed_message)
                    else:
                        print(f"Market {market} not supported for transformation.")
            except Exception as e:
                print(f"Error in pipeline: {e}")
                continue
    except KeyboardInterrupt:
        print("Pipeline interrupted by user.")
    finally:
        consumer.close()
        connection.close()

def main():
    run_pipeline()


if __name__ == "__main__":
    main()
