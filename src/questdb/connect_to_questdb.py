
import psycopg as pg
import time
# Connect to an existing QuestDB instance
conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'    

def create_sampled_trades_by_minute_binance_table() -> None:
    cur.execute("""CREATE TABLE sampled_trades_by_minute_binance  (
                symbol symbol,
                ts timestamp,
                avg_price double
                ) timestamp(ts) PARTITION BY DAY WAL
                DEDUP UPSERT KEYS(symbol, ts);
                    """)

def insert_data_into_sampled_trades_by_minute_binance_table() -> None:
    cur.execute(""" INSERT INTO sampled_trades_by_minute_binance  
                SELECT symbol, timestamp, avg(price)
                FROM "trades-BINANCE"
                SAMPLE BY 1m;         
                    """)
    



with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
        insert_data_into_sampled_trades_by_minute_binance_table()
        cur.execute("""SELECT * FROM sampled_trades_by_minute_binance  
                    """)
        records = cur.fetchall()
        for row in records:
            print(row)    
         
  
                

        
       