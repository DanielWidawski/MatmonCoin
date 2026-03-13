
import psycopg as pg
import time

    
def create_binance_trades_materilized_view(materilized_view_name: str, sample_by: str) -> None:
   cur.execute(f""" CREATE MATERIALIZED VIEW {materilized_view_name} REFRESH IMMEDIATE AS (
                    SELECT symbol, timestamp, avg(price) AS avg_price, min(price) AS min_price, max(price) AS max_price
                    FROM "trades"
                    SAMPLE BY {sample_by}
               ) PARTITION BY DAY TTL 7 DAYS
                    """)
   


conn_str = 'user=admin password=quest host=0.0.0.0 port=8812 dbname=trades-BINANCE'    

sampled_by_minute_materilized_view_name: str = 'binance_trades_minutely'
sampled_by_minute: str = '1m'

sampled_by_hour_materilized_view_name: str = 'binance_trades_hourly'
sampled_by_hour: str = '1h'

sampled_by_day_materilized_view_name: str = 'sampled_trades_by_daily'
sampled_by_day: str = '1d'



with pg.connect(conn_str, autocommit=True) as connection:
     with connection.cursor() as cur:
       #create_binance_trades_materilized_view(materilized_view_name=sampled_by_hour_materilized_view_name, sample_by=sampled_by_hour)
       cur.execute(f"""SELECT * FROM {sampled_by_hour_materilized_view_name}  
                    """)
       records = cur.fetchall()
       for row in records:
         print(row)    
                 
       