import json
import time
from datetime import datetime, timezone
import yfinance as yf
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "stock_prices_v2"
TICKERS = ["AAPL", "MSFT", "GOOGL"]
POLL_INTERVAL_SECONDS = 5

def build_producer () -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def fetch_price(ticker: str) -> dict | None:
    try:
        info = yf.Ticker(ticker).fast_info
        return {
            "ticker": ticker,
            "price": float(info["lastPrice"]),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
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
                    producer.send(TOPIC, key=ticker.encode("utf-8"), value=record)
                    print(f"[producer] sent {record}")
            producer.flush()
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[producer] stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()