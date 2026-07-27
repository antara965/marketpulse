import json
import os
import time

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv
from pydantic import ValidationError

from schemas import StockPriceEvent

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "stock_prices_v2"
GROUP_ID = "stock_price_writer"

PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST"),
    port=int(os.getenv("POSTGRES_PORT")),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

INSERT_SQL = """
    INSERT INTO fact_stock_price (ticker, price, fetched_at)
    VALUES (%(ticker)s, %(price)s, %(fetched_at)s)
"""

def parse_message(raw_bytes: bytes) -> dict | None:
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        return None

def build_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    return consumer


def main():
    consumer = build_consumer()
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    print(f"[consumer] listening on '{TOPIC}', writing into fact_stock_price. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue

            try:
                raw = parse_message(msg.value())
            except Exception:
                raw = None 

            if raw is None:
                print("[consumer] skipped a non-JSON message")
                continue

            try:
                event = StockPriceEvent(**raw)
            except ValidationError as e:
                print(f"[consumer] REJECTED bad record: {raw} -> {e}")
                continue

            try:
                cur.execute(INSERT_SQL, {
                    "ticker": event.ticker,
                    "price": float(event.price),
                    "fetched_at": event.fetched_at,
                })
                print(f"[consumer] wrote {event.ticker} @ {event.price}")
            except psycopg2.errors.ForeignKeyViolation:
                conn.rollback()
                print(f"[consumer] REJECTED unknown ticker not in dim_stock: {event.ticker}")

    except KeyboardInterrupt:
        print("\n[consumer] stopped.")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()