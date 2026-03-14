from abc import ABC, abstractmethod
import json


class Transformer(ABC):
    def get_original_message(message):
        message = message.get('message')
        if type(message.get('message')) == str:
            message = json.loads(message.get('message'))
        return message


    def get_original_data(message):
        date = message.get("data")
        if type(date) == str:
            date = json.loads(date)
        return date
    
    @abstractmethod
    def transform(self, message):
        ...
    
    

class BinanceTransformer(Transformer):
    def transform(self, message):
        new_schema = {}
        # message = {'market': 'Binance', 'message': '{"stream":"solusdt@trade","data":{"e":"trade","E":1773087732411,"T":1773087732411,"s":"SOLUSDT","t":3216441787,"p":"85.7800","q":"10.71","X":"MARKET","m":false}}'}
        # message2 = {'market': 'Binance', 'message': {'stream': 'btcusdt@aggTrade', 'data': {'e': 'aggTrade', 'E': 1773136021199, 'a': 3190727436, 's': 'BTCUSDT', 'p': '70974.50', 'q': '0.002', 'nq': '0.002', 'f': 7413816280, 'l': 7413816280, 'T': 1773136021194, 'm': False}}}
        new_schema['market'] = message.get("market")
        message = self.get_original_message(message)
        data = self.get_original_data(message)
        # print(new_schema)
        new_schema['symbol'] = data.get("s")
        new_schema['side'] = "sell" if data.get('m') else "buy"
        new_schema['price'] = data.get("p")
        new_schema['amount'] = data.get("q")
        new_schema['timestamp'] = data.get("T") / 1000.0
        return new_schema
    
class BybitTransformer(Transformer):
    def transform(self, message):
        raise NotImplementedError

    
