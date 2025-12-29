import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2
import json
from datetime import datetime
import time

print ("Connecting to database...")

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

print("The connection was successfull")

cursor = conn.cursor()

producer = None

while producer is None:

    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                 value_serializer=lambda m: json.dumps(m).encode('utf-8')
                                )
    except NoBrokersAvailable:
        print("Kakfa not ready, retrying in 5 seconds") 
        time.sleep(5)

last_seen = None

def convert_row_to_event(row):
    return {
        "id": row[0],
        "user_id": row[1],
        "amount": float(row[2]),
        "status": row[3],
        "updated_at": row[4].isoformat(),
        "deleted_at": row[5].isoformat() if row[5] else None
    }

while True:
    if last_seen is None:
        cursor.execute("SELECT id, user_id, amount, status, updated_at, deleted_at " \
                        "from orders ORDER BY updated_at")
        rows = cursor.fetchall()
        event_time = datetime.utcnow().isoformat()
        for row in rows:
            last_seen = row[4]
            event = {
                "op": "INSERT",
                "data": convert_row_to_event(row),
                "event_time": event_time
            }
            producer.send("orders_cdc", event)
    else:
        cursor.execute("SELECT id, user_id, amount, status, updated_at, deleted_at " \
                        "from orders WHERE updated_at > %s ORDER BY updated_at",
                        (last_seen, ))
        rows = cursor.fetchall()
        event_time = datetime.utcnow().isoformat()
        for row in rows:
            last_seen = row[4]
            event = {
                "op": "UPDATE",
                "data": convert_row_to_event(row),
                "event_time": event_time
            }
            producer.send("orders_cdc", event)
    
    producer.flush()
    time.sleep(5)