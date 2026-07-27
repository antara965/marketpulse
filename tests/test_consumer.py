from consumer import parse_message


def test_valid_json_is_parsed():
    result = parse_message(b'{"ticker": "AAPL", "price": 190.5}')
    assert result == {"ticker": "AAPL", "price": 190.5}


def test_invalid_json_returns_none():
    result = parse_message(b"hello kafka")
    assert result is None