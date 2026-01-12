# TCGPlayer Provider Tests

This directory contains comprehensive tests for the TCGPlayer provider (`mtgjson5/providers/tcgplayer.py`).

## Test Files

### test_auth.py (5 tests)
Tests authentication and OAuth token management:
- `test_token_success_builds_header_and_sets_api_version` - VCR test for successful auth
- `test_default_api_version_when_missing` - Default API version fallback
- `test_missing_section_logs_and_returns_empty_bearer` - Missing config section handling
- `test_missing_options_logs_and_returns_empty_bearer` - Missing credentials handling
- `test_token_post_failure_logs_error_and_returns_empty` - OAuth failure handling

**Status**: All tests implemented and passing

### test_enums.py (7 tests)
Tests enum utilities and card finish detection:
- `test_card_finish_has_value_true` - Enum value existence check (positive)
- `test_card_finish_has_value_false` - Enum value existence check (negative)
- `test_get_card_finish_finds_foil_etched` - Extract finish from card name
- `test_get_card_finish_ignores_numbers_in_parentheses` - Ignore numeric parentheses
- `test_get_card_finish_returns_none_when_no_finish` - No finish detected
- `test_convert_sku_data_enum_converts_ids_to_names` - SKU enum conversion
- `test_convert_sku_data_enum_includes_finish_when_detected` - SKU with finish field

**Status**: All tests implemented (pure logic, no VCR needed)

### test_api.py (5 tests)
Tests API download and parsing methods:
- `test_download_replaces_api_version_in_url` - VCR test for URL version replacement
- `test_get_api_results_success_parses_json` - VCR test for JSON parsing
- `test_get_api_results_empty_response_returns_empty_list` - Empty response handling
- `test_get_api_results_invalid_json_logs_error_returns_empty_list` - Invalid JSON handling
- `test_get_tcgplayer_magic_set_ids_single_page` - VCR test for set ID retrieval

**Status**: All tests implemented (3 VCR, 2 mock)

## Running Tests

### Run all TCGPlayer tests:
```bash
pytest tests/mtgjson5/providers/tcgplayer/ -v
```

### Run specific test file:
```bash
pytest tests/mtgjson5/providers/tcgplayer/test_enums.py -v
pytest tests/mtgjson5/providers/tcgplayer/test_api.py -v
pytest tests/mtgjson5/providers/tcgplayer/test_auth.py -v
```

### Run with coverage:
```bash
pytest tests/mtgjson5/providers/tcgplayer/ --cov=mtgjson5.providers.tcgplayer --cov-report=term-missing
```

## VCR Cassettes

VCR cassettes (recorded HTTP interactions) are stored in `tests/cassettes/providers/tcgplayer/`.

### Offline Testing (Default)
All tests can run offline using pre-recorded cassettes:
```bash
pytest tests/mtgjson5/providers/tcgplayer/ --record-mode=none
```

### Recording New Cassettes

**IMPORTANT**: Recording requires real TCGPlayer credentials in `mtgjson.properties`.

1. Add credentials to `mtgjson.properties`:
   ```ini
   [TCGPlayer]
   client_id=your_client_id
   client_secret=your_client_secret
   api_version=v1.39.0
   ```

2. Record all cassettes:
   ```bash
   pytest tests/mtgjson5/providers/tcgplayer/test_auth.py::test_token_success_builds_header_and_sets_api_version --record-mode=once
   pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestDownload::test_download_replaces_api_version_in_url --record-mode=once
   pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestGetApiResults::test_get_api_results_success_parses_json --record-mode=once
   pytest tests/mtgjson5/providers/tcgplayer/test_api.py::TestGetTcgplayerMagicSetIds::test_get_tcgplayer_magic_set_ids_single_page --record-mode=once
   ```

3. Verify secrets are scrubbed:
   ```bash
   # Check that cassettes contain "REDACTED" not real tokens
   grep -r "REDACTED" tests/cassettes/providers/tcgplayer/
   # Ensure no real credentials leaked
   grep -r "client_id\|client_secret" tests/cassettes/providers/tcgplayer/ || echo "✅ No credentials found"
   ```

### VCR Configuration

VCR automatically scrubs sensitive data via `conftest.py`:
- OAuth tokens (`access_token`, `userName`)
- POST credentials (`client_id`, `client_secret`)
- Authorization headers
- Volatile headers (dates, ETags, etc.)

## Test Guidelines

### When Writing New Tests

1. **Use existing fixtures**:
   - `reset_tcgplayer_singleton` - Reset singleton between tests
   - `disable_cache` - Disable requests-cache for VCR tests
   - `tcgplayer_config` - Modify config for testing

2. **VCR tests must use `disable_cache` fixture**:
   ```python
   @pytest.mark.vcr("providers/tcgplayer/test_name.yml")
   def test_something(disable_cache):
       # Test code
   ```

3. **Document cassette recording**:
   ```python
   """
   To record/update this test's cassette:
       pytest tests/mtgjson5/providers/tcgplayer/test_file.py::test_name --record-mode=all

   RECORDING: Uses real credentials from mtgjson.properties
   PLAYBACK: Uses cassette (credentials already scrubbed)
   """
   ```

4. **Prefer pure logic tests over VCR when possible** - faster and no recording needed

## Test Coverage

Current coverage: **~40%** (12 tests covering auth, enums, API download)

Coverage targets:
- **Phase 1 (DONE)**: 40% - Auth + Enums + Core API
- **Phase 2 (TODO)**: 60% - SKU mapping and pricing
- **Phase 3 (TODO)**: 75%+ - Sealed products and edge cases

## Troubleshooting

### "Cassette not found" errors
Run with `--record-mode=once` to create missing cassettes (requires real credentials).

### "Unable to contact TCGPlayer" in VCR tests
Ensure `disable_cache` fixture is used - VCR and requests-cache can conflict.

### Tests pass locally but fail in CI
Check that cassettes are committed: `git add tests/cassettes/providers/tcgplayer/`

### Real credentials in cassettes
VCR scrubbing failed - manually check cassette files and ensure `conftest.py` filters are correct.

## References

- Main module: `mtgjson5/providers/tcgplayer.py`
- VCR config: `tests/conftest.py` (see `vcr_config` fixture)
- Recording guide: `RECORDING_INSTRUCTIONS.md` (project root)
