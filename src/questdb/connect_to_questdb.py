
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

def insert_sampled_trades_data_into_a_table(sampled_by: str, table_name: str) -> None:
    cur.execute(f""" INSERT INTO {table_name}  
                SELECT symbol, timestamp, avg(price)
                FROM "trades"
                SAMPLE BY {sampled_by};     
                    """)
    

conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'    

sampled_by_hour_table_name: str = 'sampled_trades_by_hour'
sampled_by_hour: str = '1h'

sampled_by_minute_table_name: str = 'sampled_trades_by_minute'
sampled_by_minute: str = '1m'


with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
       
       #create_sampled_trades_binance_table(smapled_table_name=sampled_by_minute_table_name)
       #insert_sampled_trades_data_into_a_table(sampled_by=sampled_by_minute, table_name=sampled_by_minute_table_name)
       cur.execute(f"""SELECT * FROM {sampled_by_minute_table_name}  
                    """)
       records = cur.fetchall()
       for row in records:
         print(row)    
                 
       