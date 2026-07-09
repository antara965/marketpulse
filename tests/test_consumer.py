from consumer import safe_json_deserializer


def test_valid_json_is_parsed():
    result = safe_json_deserializer(b'{"ticker": "AAPL", "price": 190.5}')
    assert result == {"ticker": "AAPL", "price": 190.5}


def test_invalid_json_returns_none():
    result = safe_json_deserializer(b"hello kafka")
    assert result is None