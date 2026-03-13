
import psycopg as pg
import time

def create_sampled_trades_binance_table(smapled_table_name: str) -> None:
    cur.execute(f"""CREATE TABLE {smapled_table_name}  (
                symbol symbol,
                ts timestamp,
                avg_price double
                ) timestamp(ts) PARTITION BY DAY WAL
                DEDUP UPSERT KEYS(symbol, ts);
                    """)

def insert_sampled_trades_by_minute_data_into_binance_table(sampled_by: str, table_name: str) -> None:
    cur.execute(f""" INSERT INTO {table_name}  
                SELECT symbol, timestamp, avg(price)
                FROM "trades-BINANCE"
                SAMPLE BY {sampled_by};     
                    """)
    

conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'    


with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
       table_name: str = 'sampled_trades_by_hour'
       sampled_by: str = '1h'
       #create_sampled_trades_binance_table(smapled_table_name=table_name)
       insert_sampled_trades_by_minute_data_into_binance_table(sampled_by=sampled_by, table_name=table_name)
      
         
  
                

        
       