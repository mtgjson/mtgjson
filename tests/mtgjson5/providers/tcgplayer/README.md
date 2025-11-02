# TCGplayer Provider Tests

## Running Tests

Most tests run offline without network access using monkeypatch:

```bash
python -m pytest tests/mtgjson5/providers/tcgplayer/test_auth.py -q --record-mode=none
```

## Recording the VCR Cassette

**⚠️ IMPORTANT**: The `test_token_success_builds_header_and_sets_api_version` test **requires recording with real credentials**.

Dummy credentials will fail - TCGplayer validates credentials before returning a token.

### Prerequisites

1. Add **real** TCGplayer credentials to `mtgjson5/resources/mtgjson.properties`:
   ```ini
   [TCGPlayer]
   client_id=<your_real_client_id>
   client_secret=<your_real_client_secret>
   api_version=v1.39.0
   ```

### Recording

```bash
python -m pytest tests/mtgjson5/providers/tcgplayer/test_auth.py::test_token_success_builds_header_and_sets_api_version --record-mode=once -q
```

This makes **one real OAuth call** to TCGplayer and records the response.

### Verification

After recording, verify the cassette has scrubbed secrets:

```bash
cat tests/cassettes/test_token_success_builds_header_and_sets_api_version.yaml
```

Check that:
- `access_token` in response body is `"REDACTED"`
- POST form parameters `grant_type`, `client_id`, `client_secret` are filtered
- No actual credentials appear anywhere

Then commit the cassette:

```bash
git add tests/cassettes/test_token_success_builds_header_and_sets_api_version.yaml
```

## Test Coverage

- **test_token_success_builds_header_and_sets_api_version**: VCR-backed happy path (requires cassette)
- **test_default_api_version_when_missing**: Default version fallback (monkeypatch)
- **test_missing_section_logs_and_returns_empty_bearer**: Missing config section handling
- **test_missing_options_logs_and_returns_empty_bearer**: Missing credentials handling  
- **test_token_post_failure_logs_error_and_returns_empty**: Failed HTTP request handling
