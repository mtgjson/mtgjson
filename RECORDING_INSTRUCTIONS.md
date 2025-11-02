# TCGPlayer VCR Cassette Recording Instructions

## Quick Start

**ONE test needs recording with real credentials**: `test_token_success_builds_header_and_sets_api_version`

The other 4 tests work offline without recording.

## Prerequisites

Install test dependencies (includes `pytest-recording` and `vcrpy`):

```bash
pip install -r requirements_test.txt
```

## Steps

1. **Set credentials as environment variables** (keeps them out of config file):

   ```bash
   export TCGPLAYER_CLIENT_ID="your_real_client_id"
   export TCGPLAYER_CLIENT_SECRET="your_real_client_secret"
   ```

2. **Record the cassette** (makes ONE real OAuth call):

   ```bash
   python -m pytest tests/mtgjson5/providers/tcgplayer/test_auth.py::test_token_success_builds_header_and_sets_api_version --record-mode=once -q
   ```

3. **Verify secrets are scrubbed**:

   ```bash
   cat tests/cassettes/test_token_success_builds_header_and_sets_api_version.yaml
   ```

   Check for:
   - ✅ `body: ''` (POST parameters scrubbed)
   - ✅ `"access_token": "REDACTED"` (token scrubbed)
   - ✅ `code: 200` (successful response)
   - ❌ No real credentials visible

4. **Run all tests offline**:

   ```bash
   python -m pytest tests/mtgjson5/providers/tcgplayer/test_auth.py -q --record-mode=none
   ```

   Expected: **5 passed**

5. **Commit the cassette**:

   ```bash
   git add tests/cassettes/test_token_success_builds_header_and_sets_api_version.yaml
   git commit -m "Add TCGPlayer auth VCR cassette"
   ```

## Why Real Credentials Are Required

TCGPlayer validates OAuth credentials **before** returning a token. Dummy/fake credentials get a 400 Bad Request, which can't be used for testing the success path.

## Security Note

✅ **The test fixture now clears ALL config sections during recording** to prevent other API keys from leaking into cassettes.

The VCR filters automatically scrub:
- POST parameters (credentials)
- Response tokens
- Authorization headers

**The committed cassette is safe** - it contains only test data with `REDACTED` placeholders.

## Current Test Status

✅ **4 tests pass** (offline, no recording needed):
- `test_default_api_version_when_missing`
- `test_missing_section_logs_and_returns_empty_bearer`
- `test_missing_options_logs_and_returns_empty_bearer`
- `test_token_post_failure_logs_error_and_returns_empty`

⏳ **1 test needs cassette**:
- `test_token_success_builds_header_and_sets_api_version`

## Questions?

See `tests/mtgjson5/providers/tcgplayer/README.md` for full documentation.
