import json
import psycopg2
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pydantic import ValidationError
from schemas import StockPriceEvent

load_dotenv()

BOOTSTRAP_SERVERS = ["localhost:9092"]
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
      INSERT INTO stock_prices_v2 (ticker, price, fetched_at)
      VALUES (%(ticker)s, %(price)s, %(fetched_at)s)
"""

def safe_json_deserializer(v):
    try:
        return json.loads(v.decode("utf-8"))
    except json.JSONDecodeError:
        return None

def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=safe_json_deserializer,
    )

def main():
    consumer = build_consumer()
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    print(f"[consumer] listening on '{TOPIC}', writing into Postgres. Ctrl+C to stop.")
    try:
        for message in consumer:
            raw = message.value

            if raw is None:
                print("[consumer] skipped a non-JSON message")
                continue
            try:
                event = StockPriceEvent(**raw)
            except ValidationError as e:
                print(f"[consumer] REJECTED bad record: {raw} -> {e}")
                continue

            cur.execute(INSERT_SQL, {
                "ticker": event.ticker,
                "price": float(event.price),
                "fetched_at": event.fetched_at,
            })
            print(f"[consumer] wrote {event.ticker} @ {event.price}")
    except KeyboardInterrupt:
        print("\n[consumer] stopped.")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main() 