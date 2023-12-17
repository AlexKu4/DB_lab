import time
import psycopg2
import sqlite3
import duckdb
import pandas as pd

count = 10  
print("Choose data:\n 1 - tiny\n 2 - big")
d = int(input())
print("Choose library:\n 1 - psycopg\n 2 - sqlite\n 3 - duckdb\n 4 - pandas")
n = int(input())
print("Enter number of query")
k = int(input())
if n == 1:
    try:
        connection = psycopg2.connect(
            dbname = 'postgres',
            user = 'postgres',
            password = 'password',
            host = 'localhost'
            )
        cursor = connection.cursor()
        if k == 1:
            #query1
            start_time = time.time()
            for i in range(count):
                cursor.execute("SELECT VendorID, count(*) FROM trips GROUP BY 1;")
            print(f"average time of work psycopg query{k}:", (time.time() - start_time)/count)
        if k == 2:
            #query2
            start_time = time.time()
            for i in range(count):
                cursor.execute("SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;")
            print(f"average time of work psycopg query{k}:", (time.time() - start_time)/count)
        if k == 3:
            #query3
            start_time = time.time()
            for i in range(count):
                cursor.execute("SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;")
            print(f"average time of work psycopg query{k}:", (time.time() - start_time)/count)
        if k == 4:
            #query4
            start_time = time.time()
            for i in range(count):
                cursor.execute('''SELECT
                    passenger_count,
                    extract(year from tpep_pickup_dateti),
                    round(trip_distance),
                    count(*)
                    FROM trips
                    GROUP BY 1, 2, 3
                    ORDER BY 2, 4 desc;''')
            print(f"average time of work psycopg query{k}:", (time.time() - start_time)/count)
    except Exception as ex_:
        print("[INFO] Delo dryan", ex_)
    finally:
        connection.close()
        print("[INFO] PostgreSQL connection closed")

elif n == 2:
    if d == 1:
        connection = sqlite3.connect('my_db.db') 
    elif d == 2:
        connection = sqlite3.connect('my_db_big.db') 
    cursor = connection.cursor()
    start_time = time.time()
    if k == 1:
         for i in range(count):
              cursor.execute('SELECT VendorID, count(*) FROM trips GROUP BY 1;')
              n = cursor.fetchall()
    elif k == 2:
        for i in range(count):
            cursor.execute('''SELECT passenger_count, avg(total_amount)  
                FROM trips          
                GROUP BY 1;''')
            n = cursor.fetchall()
    elif k == 3:
        for i in range(count):
            cursor.execute('''SELECT passenger_count, tpep_pickup_datetime, count(*) FROM trips
                GROUP BY 1, 2;''')
            n = cursor.fetchall()
    elif k == 4:
        for i in range(count):
            cursor.execute('''SELECT passenger_count, tpep_pickup_datetime, round(trip_distance), count(*)
                   FROM trips
                   GROUP BY 1, 2, 3
                   ORDER BY 2, 4 desc;''')
            n =  cursor.fetchall()
    print(f"average time of work sqlite query{k}:", (time.time()-start_time)/count)
elif n == 3:
    conn = duckdb.connect()
    if d == 1:
        df = pd.concat([pd.read_csv("nyc_yellow_tiny.csv")]) 
    elif d == 2:
        df = pd.concat([pd.read_csv("nyc_yellow_big.csv")])
    start_time = time.time()
    if k == 1:
        for i in range(count):
            df = conn.execute('''SELECT VendorID, count(*) FROM "nyc_yellow_tiny.csv" GROUP BY 1;''').df() 
    elif k == 2:
            for i in range(count):
                df = conn.execute('''SELECT passenger_count, avg(total_amount) FROM "nyc_yellow_tiny.csv" GROUP BY 1;''').df()
    elif k == 3:
        for i in range(count):
            df = conn.execute('''SELECT
                passenger_count, 
                extract(year from tpep_pickup_datetime),
                count(*)
                FROM "nyc_yellow_tiny.csv"
                GROUP BY 1, 2;''').df()
    elif k == 4:
        for i in range(count):
            df = conn.execute('''SELECT
                passenger_count,
                extract(year from tpep_pickup_datetime),
                round(trip_distance),
                count(*)
                FROM "nyc_yellow_tiny.csv"
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 desc;''').df()   
    print(f"average time of work duckdb query{k}:", (time.time()-start_time)/count)

if n == 4:
    if d == 1:
        trips = pd.read_csv("nyc_yellow_tiny.csv")
    elif d == 2:
        trips = pd.read_csv("nyc_yellow_big.csv")
    start_time = time.time()
    if k == 1:
        for i in range(count):
            selected_df = trips[['payment_type']]
            grouped_df = selected_df.groupby('payment_type')
            final_df = grouped_df.size().reset_index(name='counts')
    elif k == 2:
        for i in range(count):
            selected_df = trips[['passenger_count', 'total_amount']] 
            grouped_df = selected_df.groupby('passenger_count') 
            final_df = grouped_df.mean().reset_index()
    elif k == 3:
        for i in range(count):
            selected_df = trips[['passenger_count', 'tpep_pickup_datetime']] 
            selected_df['year'] = pd.to_datetime(
                selected_df.pop('tpep_pickup_datetime'),
                format='%Y-%m-%d %H:%M:%S').dt.year
            grouped_df = selected_df.groupby(['passenger_count', 'year']) 
            final_df = grouped_df.size().reset_index(name='counts')
    elif k == 4:
        for i in range(count):
            selected_df = trips[[
                'passenger_count', 
                'tpep_pickup_datetime', 
                'trip_distance']]
            selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
            selected_df['year'] = pd.to_datetime(
                selected_df.pop('tpep_pickup_datetime'),
                format='%Y-%m-%d %H:%M:%S').dt.year
            grouped_df = selected_df.groupby([
                'passenger_count',
                'year',
                'trip_distance'])
            final_df = grouped_df.size().reset_index(name='counts')
            final_df = final_df.sort_values(['year', 'counts'], ascending=[True, False])
    print(f"average time of work pandas query{k}:", (time.time()-start_time)/count)
