import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "stock_prices_v2"
TICKERS = ["AAPL", "MSFT", "GOOGL"]
POLL_INTERVAL_SECONDS = 5

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
FINNHUB_URL = "https://finnhub.io/api/v1/quote"

session = requests.Session()


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}")
    else:
        print(f"[producer] delivered to {msg.topic()} [{msg.partition()}]")


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def fetch_price(ticker: str) -> dict | None:
    try:
        response = session.get(
            FINNHUB_URL,
            params={"symbol": ticker, "token": FINNHUB_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        current_price = data.get("c")
        if not current_price:
            print(f"[producer] no price data for {ticker}: {data}")
            return None

        return {
            "ticker": ticker,
            "price": float(current_price),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except requests.RequestException as e:
        print(f"[producer] could not fetch {ticker}: {e}")
        return None


def main():
    producer = build_producer()
    print(f"[producer] sending to topic '{TOPIC}' every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")

    try:
        while True:
            for ticker in TICKERS:
                record = fetch_price(ticker)
                if record:
                    producer.produce(
                        TOPIC,
                        key=ticker.encode("utf-8"),
                        value=json.dumps(record).encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    print(f"[producer] sent {record}")
            producer.flush()
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[producer] stopped.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()