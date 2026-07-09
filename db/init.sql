CREATE TABLE IF NOT EXISTS dim_stock (
    ticker        TEXT PRIMARY KEY,
    company_name  TEXT NOT NULL,
    sector        TEXT
);

CREATE TABLE IF NOT EXISTS fact_stock_price (
    id          SERIAL PRIMARY KEY,
    ticker      TEXT NOT NULL REFERENCES dim_stock(ticker),
    price       NUMERIC(12, 4) NOT NULL,
    fetched_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_stock_price_ticker_time
    ON fact_stock_price (ticker, fetched_at);

INSERT INTO dim_stock (ticker, company_name, sector) VALUES
    ('AAPL', 'Apple Inc.', 'Technology'),
    ('MSFT', 'Microsoft Corporation', 'Technology'),
    ('GOOGL', 'Alphabet Inc.', 'Technology')
ON CONFLICT (ticker) DO NOTHING;
