# MTGJSON Test Suite

This directory contains the test suite for the MTGJSON project, which builds comprehensive Magic: The Gathering card data files from various third-party providers.

## What These Tests Do

The test suite validates core MTGJSON functionality:

- **Card Data Building** - Tests that card objects are correctly constructed with proper attributes (UUIDs, names, types, foreign data, legalities, prices, etc.)
- **Set Construction** - Validates set-level operations like DFC (Double-Faced Card) handling, token generation, and booster pack composition
- **Output Generation** - Ensures compiled JSON files meet schema requirements and contain expected data
- **Provider Integration** - Tests API interactions with third-party providers (Scryfall, TCGplayer, Cardmarket) using recorded HTTP fixtures
- **Price Data** - Validates price aggregation from multiple vendors
- **Data Normalization** - Tests text normalization, name parsing, and data transformation utilities

**Current Coverage**: 113/118 tests passing (96% pass rate), 58% overall code coverage

## Test Organization

### Directory Structure

```
tests/
├── README.md                           # This file
├── conftest.py                         # Shared pytest fixtures and configuration
├── pytest.ini                          # Pytest settings (VCR.py config, test paths)
├── cassettes/                          # VCR.py HTTP interaction recordings
│   └── providers/
│       ├── scryfall/                   # Scryfall API recordings
│       └── tcgplayer/                  # TCGplayer API recordings
└── mtgjson5/                           # Test modules mirroring source structure
    ├── test_set_builder.py             # Core set building tests (49% coverage)
    ├── test_output_generator.py        # Output compilation tests (63% coverage)
    ├── test_today_price_builder.py     # Price aggregation tests (77% coverage)
    ├── test_card_sorting.py            # Card ordering tests
    ├── test_oracle_id_populates.py     # Oracle ID validation
    ├── test_name_parts_match_expected.py # Name parsing tests
    ├── providers/
    │   ├── scryfall/
    │   │   └── test_monolith.py        # Scryfall provider tests (78% coverage)
    │   └── tcgplayer/
    │       ├── test_auth.py            # TCGplayer authentication tests
    │       └── test_enums.py           # TCGplayer enum/finish detection tests
    └── resources/
        └── today_price_builder/         # Test fixtures for price builder
```

### Naming Conventions

- **Test files**: `test_<module_name>.py` mirrors the source module being tested
- **Test classes**: `Test<FeatureName>` groups related test cases
- **Test methods**: `test_<what_is_being_tested>_<expected_outcome>` describes the test scenario
- **AAA Pattern**: Tests follow Arrange-Act-Assert structure with comments

### VCR Cassettes

HTTP interactions with external APIs are recorded as "cassettes" using VCR.py:

- **Location**: `tests/cassettes/providers/<provider_name>/`
- **Format**: YAML files containing request/response data
- **Benefits**: Tests run offline without hitting real APIs, ensuring consistent results
- **Security**: OAuth credentials are automatically scrubbed from recordings

## How to Run Tests

### Initial Setup

1. **Create virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements_test.txt
   ```

### Running Tests

**Run all tests**:
```bash
PYTHONPATH=. pytest tests/ -v
```

**Run specific test file**:
```bash
PYTHONPATH=. pytest tests/mtgjson5/test_set_builder.py -v
```

**Run specific test class**:
```bash
PYTHONPATH=. pytest tests/mtgjson5/test_set_builder.py::TestSetBuilder -v
```

**Run specific test method**:
```bash
PYTHONPATH=. pytest tests/mtgjson5/providers/tcgplayer/test_auth.py::TestTokenSuccess::test_token_success_builds_header_and_sets_api_version -v
```

**Run with coverage report**:
```bash
PYTHONPATH=. pytest tests/ --cov=mtgjson5 --cov-report=term-missing
```

**Run specific coverage target**:
```bash
PYTHONPATH=. pytest tests/ --cov=mtgjson5.set_builder --cov-report=term-missing
```

### Understanding Output

- **Green dots (.)**: Tests passing
- **Red F**: Test failures
- **Yellow s**: Skipped tests
- **Coverage %**: Percentage of code lines executed during tests

### Common Issues

**ModuleNotFoundError**: Ensure `PYTHONPATH=.` is set when running pytest

**Missing cassettes**: Some tests require VCR cassettes that need real API credentials to record (currently 5 tests fail due to missing cassettes)

**Singleton access**: When testing singleton classes, access static methods via `ClassName.__wrapped__.method_name`

## Test Coverage Highlights

| Module | Coverage | Status |
|--------|----------|--------|
| tcgplayer provider | 77% | ✓ Strong |
| scryfall/monolith | 78% | ✓ Strong |
| today_price_builder | 77% | ✓ Strong |
| output_generator | 63% | ✓ Good |
| set_builder | 49% | ⚠ Moderate |

**Overall**: 58% code coverage across 113 passing tests

## Contributing New Tests

When adding tests:

1. **Mirror source structure**: Place test files in `tests/mtgjson5/` matching the source layout
2. **Use AAA pattern**: Arrange-Act-Assert with clear comments
3. **Add docstrings**: Include brief descriptions and reference source code line numbers
4. **Mock external calls**: Use VCR cassettes for API calls, mocks for file I/O
5. **Test edge cases**: Don't just test happy paths - test error conditions and boundary values
6. **Update cassettes carefully**: Never commit real API credentials or sensitive data

## Recording New VCR Cassettes

**⚠ WARNING**: Recording new cassettes requires valid API credentials for third-party providers. Do not commit credentials to the repository.

To record a new cassette:

1. Set required environment variables (API keys, secrets)
2. Run the test with `--record-mode=new_episodes`
3. Verify credentials are scrubbed from the cassette YAML
4. Commit the cassette file only if clean

See `RECORDING_INSTRUCTIONS.md` in the project root for detailed guidance.
