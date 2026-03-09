import asyncio
import json
import threading

import websockets
from socket_connection import SocketConnection


# Function to handle the chat client
async def chat():
    """ws = websocket.WebSocketApp(
        url="wss://stream.bybit.com/v5/trade"
    )

    # Authenticate with API.
    ws.send(
        json.dumps({
            "req_id": "test",
            "op": "subscribe",
            "args": ["publicTrade.BTCUSDT"]}))"""

    async with websockets.connect('wss://fstream.binance.com/stream?streams=solusdt@aggTrade') as websocket:
        async for message in websocket:
            # Broadcast the message to all other connected clients
            print(message)

# Run the client
if __name__ == "__main__":
    asyncio.run(chat())
    """url = "wss://stream.bybit.com/v5/trade/public/spot"
    topic = ["tickers.ADAUSTD", "orderbook.ADAUSTD"]
    thread = threading.Thread(target=SocketConnection, args=(url, topic)).start()"""
    #threading.Thread(target=SocketConnection, args=("wss://stream.bybit.com/v5/public/linear", ["publictrade.ETHUSDT"])).start()