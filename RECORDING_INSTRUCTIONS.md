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

1. **Add real credentials** to `mtgjson5/resources/mtgjson.properties`:

   ```ini
   [TCGPlayer]
   client_id=your_real_client_id
   client_secret=your_real_client_secret
   api_version=v1.39.0
   ```

2. **Record the cassette** (makes ONE real OAuth call):

   ```bash
   python -m pytest tests/mtgjson5/providers/tcgplayer/test_auth.py::test_token_success_builds_header_and_sets_api_version --record-mode=once -q
   ```

3. **Verify secrets are scrubbed**:

   ```bash
   cat tests/cassettes/providers/tcgplayer/test_token_success_builds_header_and_sets_api_version.yml
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
   git add tests/cassettes/providers/tcgplayer/test_token_success_builds_header_and_sets_api_version.yml
   git commit -m "Add TCGPlayer auth VCR cassette"
   ```

## Why Real Credentials Are Required

TCGPlayer validates OAuth credentials **before** returning a token. Dummy/fake credentials get a 400 Bad Request, which can't be used for testing the success path.

## Security Note

The VCR filters automatically scrub:
- POST parameters (credentials)
- Response tokens  
- Authorization headers

**The committed cassette is safe** - it contains only `REDACTED` placeholders.

Always review the cassette file before committing to verify scrubbing worked correctly.

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
