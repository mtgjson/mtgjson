# TCGplayer Provider Tests

This directory contains tests for the TCGplayer API integration.

## Testing Approach

Tests use `pytest-mock` (already in `requirements_test.txt`) to mock HTTP responses, avoiding live API calls during testing.

### Why pytest-mock Instead of VCR or requests-cache?

1. **Simplicity**: Direct mocking is simpler than managing cassettes or cache databases
2. **No external files**: Test data lives in the test file, making it easy to understand and modify
3. **Fast**: No I/O overhead from reading cassettes or cache files
4. **Already available**: pytest-mock is already a dependency

### How It Works

The `mock_tcgplayer_api` fixture (in `test_tcgplayer_prices_map_shape.py`) patches the `TCGPlayerProvider.get_api_results()` method to return mock data instead of making real HTTP calls:

```python
@pytest.fixture(autouse=True)
def mock_tcgplayer_api(mocker):
    """Mock TCGplayer API responses for all tests in this module."""
    mock_data = [...]  # Test response data
    
    def get_api_results_mock(url, params=None):
        if "3094" in url:
            return mock_data
        return []
    
    provider_instance = TCGPlayerProvider()
    mocker.patch.object(provider_instance, "get_api_results", get_api_results_mock)
```

### Singleton Considerations

`TCGPlayerProvider` uses the `@singleton` decorator. To mock its methods:
1. Instantiate the provider to get the singleton instance
2. Use `mocker.patch.object(instance, "method_name", mock_func)` to patch the instance method

### Running Tests

```bash
# Run all provider tests
PYTHONPATH=. pytest tests/mtgjson5/providers/ -v

# Run specific test file
PYTHONPATH=. pytest tests/mtgjson5/providers/test_tcgplayer_prices_map_shape.py -v

# Run specific test
PYTHONPATH=. pytest tests/mtgjson5/providers/test_tcgplayer_prices_map_shape.py::test_get_tcgplayer_prices_map_shape -v
```

### Adding New Tests

To test functions that call TCGplayer APIs:

1. Create test file in `tests/mtgjson5/providers/`
2. Add `mock_tcgplayer_api` fixture (or similar) that patches `get_api_results()`
3. Return appropriate mock data based on URL patterns
4. Write assertions against expected return values and data shapes

Example:

```python
@pytest.fixture(autouse=True)
def mock_api(mocker):
    def mock_results(url, params=None):
        if "some-endpoint" in url:
            return [{"id": 1, "data": "test"}]
        return []
    
    provider = TCGPlayerProvider()
    mocker.patch.object(provider, "get_api_results", mock_results)

def test_my_function():
    result = get_tcgplayer_prices_map(...)
    assert isinstance(result, dict)
    # ... more assertions
```

### Test Coverage

Current tests verify:
- Return type and structure of `get_tcgplayer_prices_map()`
- Metadata fields (source, provider, currency, date)
- Field mapping logic (Normal→sell_normal, Foil→sell_foil, Etched→sell_etched)
- Buylist fields remain unset (function only handles retail prices)
- Empty results handling

### Future Improvements

- Add tests for `get_tcgplayer_buylist_prices_map()`
- Test SKU mapping logic
- Test sealed product handling
- Test error conditions (malformed JSON, network errors, etc.)
