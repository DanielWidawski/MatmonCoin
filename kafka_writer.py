import json

from confluent_kafka import Producer
from cryptofeed import FeedHandler
from cryptofeed.backends.quest import CandlesQuest, TradeQuest
from cryptofeed.defines import CANDLES, TRADES
from cryptofeed.exchanges import Binance, Bybit, Coinbase, Gemini

config = {
    # User-specific properties that you must set
    'bootstrap.servers': 'localhost:9092',

    # Fixed properties
    'acks': 'all'
}
topic = "my-topic"
producer = Producer(config)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        pass


async def kafka_writer(t, receipt_timestamp):
    #print(t.raw)
    producer.produce(topic, (json.dumps({"market": "Binance", "message": {"data": t.raw}})).encode('utf-8'), callback=delivery_report)
    producer.flush()


def main():
    config = {'log': {'filename': 'demo.log', 'level': 'DEBUG', 'disabled': False}}
    f = FeedHandler(config=config)
    binance_symbols = ['SOL-USDT', 'BTC-USDT', 'ETH-USDT', 'ARB-USDT', 'OP-USDT', 'PEPE-USDT', 'WIF-USDT', 'BNB-USDT']
    f.add_feed(Binance(channels=[TRADES], symbols=binance_symbols, callbacks={TRADES: kafka_writer}))
    # f.add_feed(Bybit(symbols=['BTC-USDT-PERP'], channels=[TRADES], callbacks={TRADES: kafka_writer}))

    f.run()


# Run the client
if __name__ == "__main__":
    main()