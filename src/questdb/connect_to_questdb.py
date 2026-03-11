
import psycopg as pg
import time
# Connect to an existing QuestDB instance
conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'
with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
         cur.execute("""CREATE TABLE sampled_trades_by_minute_binance (
                    symbol symbol,
                    ts timestamp,
                    avg_price double) timestamp(ts) PARTITION BY DAY TTL 7 DAYS;
                     
                    ALTER TABLE sampled_trades_by_minute_binance DEDUP DISABLE;
                     
                    INSERT INTO "sampled_trades_by_minute_binance"
                    SELECT symbol, timestamp, avg(price)
                    FROM "trades-BINANCE"
                    SAMPLE BY 1m
                    FILL(NULL, NULL, NULL);  
                     
                    
    
                    """)
                

        
       