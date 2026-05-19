import pandas as pd
import time
from datetime import datetime
from clickhouse_driver import Client

client = Client(host='localhost', user='default', password='root', port=9000)

print("Проверяем подключение и создаем таблицу...")
client.execute('''
    CREATE TABLE IF NOT EXISTS default.taxi_trips (
        pickup_datetime DateTime,
        dropoff_datetime DateTime,
        passenger_count Int16,
        trip_distance Float32,
        pulocationid Int32,
        dolocationid Int32,
        total_amount Float32
    ) ENGINE = MergeTree()
    ORDER BY pickup_datetime
''')

print("Читаем Parquet файл...")
df = pd.read_parquet('yellow_tripdata_2026-01.parquet')

print(f"Строк до очистки: {len(df)}")

# бесплатные/отрицательные поездки и поездки без пассажиров
df = df[(df['total_amount'] > 0) & (df['passenger_count'] > 0)]

columns_to_keep = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'PULocationID', 'DOLocationID', 'total_amount'
]
df = df[columns_to_keep]

df = df.dropna()

print(f"Строк после очистки: {len(df)}")

print("Начинаем генерацию потока в ClickHouse.")

BATCH_SIZE = 1000
batch_data = []

try:
    for index, row in df.iterrows():
        trip_duration = row['tpep_dropoff_datetime'] - row['tpep_pickup_datetime']
        
        now = datetime.now()
        new_pickup = now
        new_dropoff = now + trip_duration

        trip = (
            new_pickup,
            new_dropoff,
            int(row['passenger_count']),
            float(row['trip_distance']),
            int(row['PULocationID']),
            int(row['DOLocationID']),
            float(row['total_amount'])
        )
        batch_data.append(trip)

        if len(batch_data) >= BATCH_SIZE:
            client.execute(
                'INSERT INTO default.taxi_trips VALUES', 
                batch_data
            )
            print(f"[{now.strftime('%H:%M:%S')}] Отправлено поездок: {BATCH_SIZE}")
            
            batch_data = []
            
            time.sleep(1)

except KeyboardInterrupt:
    print("\nГенерация остановлена вручную.")
except Exception as e:
    print(f"\nПроизошла ошибка: {e}")