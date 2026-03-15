import src.transform as transform

market_transformer: dict[str, transform.Transformer] = {}

market_transformer['BINANCE'] = transform.BinanceTransformer()
market_transformer['BYBIT'] = transform.BybitTransformer()
