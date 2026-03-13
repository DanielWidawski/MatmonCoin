
import psycopg as pg
import time

def create_sampled_trades_binance_materialized_view(smapled_table_name: str) -> None:
    cur.execute(f"""CREATE MATERIALIZED VIEW {smapled_table_name}  (
                symbol symbol,
                ts timestamp,
                avg_price double
                ) timestamp(ts) PARTITION BY DAY TTL 7 DAYS
                DEDUP UPSERT KEYS(symbol, ts);
                    """)

def insert_sampled_trades_data_into_a_table(sampled_by: str, table_name: str) -> None:
    cur.execute(f""" INSERT INTO {table_name}  
                SELECT symbol, timestamp, avg(price)
                FROM "trades"
                SAMPLE BY {sampled_by};     
                    """)
    
def create_binance_trades_materilized_view(materilized_view_name: str, sample_by: str) -> None:
   cur.execute(f""" CREATE MATERIALIZED VIEW {materilized_view_name} REFRESH IMMEDIATE AS (
                    SELECT symbol, timestamp, avg(price) AS avg_price 
                    FROM "trades"
                    SAMPLE BY 1m 
               ) PARTITION BY DAY TTL 7 DAYS
                    """)
   


conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'    

sampled_by_hour_materilized_view_name: str = 'binance_trades_hourly'
sampled_by_hour: str = '1h'

sampled_by_minute_materilized_view_name: str = 'sampled_trades_by_minute'
sampled_by_minute: str = '1m'

sampled_by_day_materilized_view_name: str = 'sampled_trades_by_day'
sampled_by_day: str = '1d'



with pg.connect(conn_str, autocommit=True) as connection:
    # Open a cursor to perform database operations
    with connection.cursor() as cur:
        #Query the database and obtain data as Python objects.
       
       #create_binance_trades_materilized_view(materilized_view_name=sampled_by_hour_materilized_view_name, sample_by=sampled_by_hour)
       cur.execute(f"""SELECT * FROM {sampled_by_hour_materilized_view_name}  
                    """)
       records = cur.fetchall()
       for row in records:
         print(row)    
                 
       