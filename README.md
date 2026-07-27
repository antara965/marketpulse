# MarketPulse — Real-Time Stock Data Pipeline

A production-shaped, containerized streaming pipeline that ingests live stock prices, validates them, and lands them in a queryable data warehouse.

## Architecture

<img width="872" height="322" alt="image" src="https://github.com/user-attachments/assets/07844b38-d110-4db4-9f4d-ae16ebcd0ed6" />

Finnhub API → Producer → Kafka → Consumer → Postgres → SQL analysis

- **Producer** (`src/producer.py`) — polls the Finnhub REST API for live stock quotes and publishes each one to a Kafka topic.
- **Kafka** — decouples ingestion from storage; buffers messages so the producer and consumer never need to run in lockstep.
- **Consumer** (`src/consumer.py`) — reads from Kafka, validates each record with Pydantic, and writes it into Postgres.
- **Postgres** — modeled as a star schema (`dim_stock` + `fact_stock_price`) with a foreign key enforcing referential integrity.
- Everything runs via a single `docker compose up` — Kafka, Postgres, producer, and consumer are all containerized.

## Tech stack

Python · Apache Kafka (KRaft mode) · `confluent-kafka` · PostgreSQL · Docker Compose · Pydantic · pytest · Finnhub API

## Running it

**Prerequisites**: Docker, Docker Compose, a free [Finnhub](https://finnhub.io) API key.

1. Copy `.env.example` to `.env` and fill in your `FINNHUB_API_KEY`.
2. Start everything:
```bash
   docker compose up --build
```
3. Check data is flowing:
```bash
   docker exec -it stock-postgres psql -U stockuser -d stockdb -c "SELECT * FROM fact_stock_price ORDER BY fetched_at DESC LIMIT 10;"
```

## Example analysis queries

See `sql/example_queries.sql` for moving averages, latest-price-per-ticker, and percent-change queries using Postgres window functions.

## Tests

```bash
pip install -r requirements.txt
pip install pytest
python -m pytest -v
```

Covers Pydantic validation rules and Kafka message deserialization.

## Project structure

```
stock-pipeline/
├── src/
│   ├── producer.py       # Fetches prices from Finnhub, publishes to Kafka
│   ├── consumer.py       # Validates and writes Kafka messages to Postgres
│   ├── schemas.py        # Pydantic data validation models
│   └── Dockerfile
├── db/
│   └── init.sql          # Star schema (dim_stock, fact_stock_price)
├── sql/
│   └── example_queries.sql
├── tests/
│   ├── test_schemas.py
│   └── test_consumer.py
├── docker-compose.yml
├── .env.example
└── requirements.txt
```
