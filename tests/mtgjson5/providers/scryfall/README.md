# Scryfall Provider Tests

Comprehensive tests for `mtgjson5/providers/scryfall/monolith.py` using VCR cassettes and mocks.

## Test Coverage

**Target:** 80-85% coverage of `mtgjson5/providers/scryfall/monolith.py`

**Current Status:** Phase 1 implemented (10 critical tests)

### Phase 1: Core Download and Pagination (10 tests)

| Test Name | Type | Status | Cassette Required |
|-----------|------|--------|-------------------|
| `test_catalog_keyword_abilities` | VCR | ✅ Existing | ✅ Recorded |
| `test_download_success` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_with_chunked_encoding_error_retries` | Mock | ✅ Implemented | N/A (uses mocks) |
| `test_download_chunked_encoding_error_max_retries` | Mock | ✅ Implemented | N/A (uses mocks) |
| `test_download_json_parsing_error_with_504` | Mock | ✅ Implemented | N/A (uses mocks) |
| `test_download_all_pages_single_page` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_all_pages_multiple_pages` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_all_pages_error_response` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_cards_success` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_cards_empty_set` | VCR | ✅ Implemented | ⏳ Needs recording |
| `test_download_cards_sorting` | VCR | ✅ Implemented | ⏳ Needs recording |

## Quick Start: Recording Cassettes

### Prerequisites

1. Install test dependencies:
   ```bash
   pip install -r requirements_test.txt
   ```

2. Ensure internet access (VCR will make real API calls to Scryfall)

### Recording All Cassettes at Once

From the project root:

```bash
pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=once -v
```

This will:
- Run all 11 tests (1 existing + 10 new)
- Record 7 new VCR cassettes (3 tests use mocks, no cassettes needed)
- Skip recording if cassette already exists (`--record-mode=once`)

**Expected Output:**
```
test_catalog_keyword_abilities PASSED (uses existing cassette)
test_download_success PASSED (records new cassette)
test_download_with_chunked_encoding_error_retries PASSED (mock test)
test_download_chunked_encoding_error_max_retries PASSED (mock test)
test_download_json_parsing_error_with_504 PASSED (mock test)
test_download_all_pages_single_page PASSED (records new cassette)
test_download_all_pages_multiple_pages PASSED (records new cassette)
test_download_all_pages_error_response PASSED (records new cassette)
test_download_cards_success PASSED (records new cassette)
test_download_cards_empty_set PASSED (records new cassette)
test_download_cards_sorting PASSED (records new cassette)
```

### Recording Individual Cassettes

If you need to record or update a specific cassette:

```bash
# Record test_download_success cassette
pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_success --record-mode=once

# Re-record (overwrite) existing cassette
pytest tests/mtgjson5/providers/scryfall/test_monolith.py::test_download_all_pages_multiple_pages --record-mode=all
```

### VCR Cassette Locations

All cassettes are stored under: `tests/cassettes/providers/scryfall/`

**New cassettes to be recorded:**
- `test_download_success.yml`
- `test_download_all_pages_single_page.yml`
- `test_download_all_pages_multiple_pages.yml`
- `test_download_all_pages_error_response.yml`
- `test_download_cards_m19.yml`
- `test_download_cards_empty_set.yml`
- `test_download_cards_sorting.yml`

**Existing cassette:**
- `test_catalog_keyword_abilities.yml` (already recorded)

## Running Tests Offline

Once cassettes are recorded, tests can run offline without network access:

```bash
# Run with existing cassettes (no recording)
pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=none

# Run with coverage report
pytest tests/mtgjson5/providers/scryfall/test_monolith.py --record-mode=none --cov=mtgjson5.providers.scryfall.monolith --cov-report=term-missing
```

## Test Details

### VCR Tests (7 tests)

These tests replay HTTP interactions from YAML cassettes for deterministic offline testing:

1. **test_download_success** - Basic successful API download
   - API: `https://api.scryfall.com/catalog/card-names`
   - Validates JSON parsing and response structure

2. **test_download_all_pages_single_page** - Single page result
   - Query: Black Lotus from Alpha (specific card)
   - Validates single-page pagination logic

3. **test_download_all_pages_multiple_pages** - Multi-page result
   - Query: War of the Spark set (~264 cards, 2+ pages)
   - Validates page incrementing and has_more handling

4. **test_download_all_pages_error_response** - Error handling
   - Query: Invalid syntax that triggers API error
   - Validates graceful error handling

5. **test_download_cards_success** - Full set download
   - Set: M19 (Core Set 2019)
   - Validates sorting and data integrity

6. **test_download_cards_empty_set** - Non-existent set
   - Set: NOTAREALSET123 (invalid code)
   - Validates error handling for missing sets

7. **test_download_cards_sorting** - Sort validation
   - Set: KHM (Kaldheim)
   - Validates name/collector_number sorting

### Mock Tests (3 tests)

These tests use `unittest.mock` to simulate errors without network calls:

1. **test_download_with_chunked_encoding_error_retries**
   - Simulates ChunkedEncodingError → retry → success
   - Validates retry logic with sleep timing

2. **test_download_chunked_encoding_error_max_retries**
   - Simulates persistent ChunkedEncodingError
   - Validates sys.exit(1) after max retries

3. **test_download_json_parsing_error_with_504**
   - Simulates 504 Gateway Timeout with invalid JSON
   - Validates retry with 5-second sleep

## Troubleshooting

### Rate Limiting

Scryfall API has rate limits (15 calls/second). The provider uses `@ratelimit` decorators to handle this automatically. During cassette recording, you may see brief pauses - this is normal.

### Cassette Recording Failures

If a VCR test fails during recording:

1. Check internet connection
2. Verify Scryfall API is accessible: `curl https://api.scryfall.com/`
3. Try re-recording with `--record-mode=all` to overwrite
4. Check for API changes in Scryfall's response format

### Test Failures with Existing Cassettes

If a test passes during recording but fails with cassettes:

1. Verify cassette file exists in `tests/cassettes/providers/scryfall/`
2. Check that `disable_cache` fixture is used (required for VCR tests)
3. Try deleting cassette and re-recording

### Mock Test Failures

Mock tests should always pass (no network dependency). If failing:

1. Check that mocks are properly configured
2. Verify import paths match actual code
3. Check for changes in monolith.py that affect mocked behavior

## VCR Configuration

VCR configuration is in `tests/conftest.py`:

- **Filtered headers:** authorization, date, etag (for cassette stability)
- **Decode compressed:** Yes (human-readable YAML cassettes)
- **Match on:** method, scheme, host, port, path, query
- **Record mode:** "once" by default, "none" in CI/offline mode

## Security Notes

Scryfall API is public and requires no authentication. All cassettes contain only public data and can be safely committed to the repository.

No sensitive data (API keys, tokens, credentials) is involved in these tests.

## Contributing

When adding new tests:

1. Follow the existing pattern (VCR for API calls, mocks for error handling)
2. Always use `disable_cache` fixture for VCR tests
3. Add clear docstrings with cassette recording instructions
4. Update this README with new test details
5. Record cassettes before committing (verify cassette files exist)

## Questions?

See main project documentation or test implementation for more details.
