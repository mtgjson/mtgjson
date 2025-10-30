# VCR Cassettes for TCGplayer Tests

This directory contains VCR.py cassette files that record HTTP interactions with the TCGplayer API for testing purposes.

## What are cassettes?

Cassettes are YAML files that store HTTP request/response pairs. They allow tests to replay API interactions without making live network calls, making tests:
- Faster (no network latency)
- More reliable (no API downtime or rate limits)
- Deterministic (same responses every time)
- Safe for CI (no credentials needed)

## Security

All cassettes have sensitive data filtered:
- `Authorization` headers are removed
- `client_id` and `client_secret` in POST bodies are replaced with `FILTERED`
- Bearer tokens in responses are replaced with `FILTERED_BEARER_TOKEN`

**Never commit real credentials to cassettes.**

## Re-recording Cassettes

When the TCGplayer API changes or you need to update test data:

### Prerequisites
1. Set your TCGplayer credentials in `mtgjson5/resources/mtgjson.properties`:
   ```ini
   [TCGPlayer]
   client_id = your_client_id_here
   client_secret = your_client_secret_here
   api_version = v1.39.0
   ```

### Re-record a specific test
```bash
# Set environment variable to force re-recording
export VCR_RECORD_MODE=all

# Run specific test
PYTHONPATH=. pytest tests/mtgjson5/providers/test_tcgplayer_prices_map_shape.py -k test_get_tcgplayer_prices_map_shape -v

# Unset when done
unset VCR_RECORD_MODE
```

### Re-record all cassettes in a file
```bash
VCR_RECORD_MODE=all PYTHONPATH=. pytest tests/mtgjson5/providers/test_tcgplayer_prices_map_shape.py -v
```

### Verify filtering worked
After re-recording, inspect the cassette files to ensure:
- No real `Authorization` headers
- POST bodies show `client_id=FILTERED` and `client_secret=FILTERED`
- Token responses show `FILTERED_BEARER_TOKEN`

```bash
# Check cassettes for leaked secrets
grep -r "client_id=" tests/mtgjson5/providers/cassettes/
# Should only show FILTERED, not real IDs
```

## Cassette Files

- `tcgplayer_prices_map.yaml` - Normal test case with sample pricing data
- `tcgplayer_prices_map_empty.yaml` - Edge case with empty API results

## CI/CD

In CI, cassettes replay automatically without credentials. The `record_mode` defaults to `'once'`, which means:
- If cassette exists → replay it
- If cassette missing → record it (will fail in CI without credentials)

To enforce no live calls in CI, set:
```bash
export VCR_RECORD_MODE=none
```

This makes tests fail if a cassette is missing rather than attempting live recording.
