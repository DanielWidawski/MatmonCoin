
import psycopg as pg
import time
# Connect to an existing QuestDB instance
conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'
with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
        cur.execute("""CREATE TABLE sampled_by_minutes_trades_binance (
                    symbol symbol,
                    ts timestamp,
                    avg_price double
                    ) timestamp(ts) PARTITION BY DAY TTL 7 DAYS;

                     ALTER TABLE sampled_by_minutes_trades_binance DEDUP DISABLE;
                                        """)
       