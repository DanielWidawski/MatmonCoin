from typing import Callable

import transform

market_transformer: dict[str, callable] = {}

market_transformer['BINANCE'] = transform.BinanceTransformer.transform
market_transformer['BYBIT'] = transform.BybitTransformer.transform
