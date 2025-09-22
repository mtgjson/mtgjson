# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

MTGJSON is a Python application that generates comprehensive Magic: The Gathering card data in JSON format. It aggregates data from multiple sources (Scryfall, Gatherer, TCGPlayer, etc.) to create structured datasets for the MTG community.

The project operates as a CLI-driven data pipeline that fetches, transforms, and outputs various JSON files including individual sets, compiled databases (AllPrintings), pricing data, and format-specific collections.

## Prerequisites & Setup

### Python Environment
- **Python 3.9-3.13** (tested across all versions)
- **asdf recommended** for Python version management: `asdf install python latest && asdf set python <version> -u`
- Virtual environment recommended: `python -m venv venv && source venv/bin/activate`
- Install dependencies: `pip install --upgrade pip && pip install -r requirements.txt`
- Development dependencies: `pip install -r requirements_test.txt`

### Quick Setup (Complete Environment)
```bash
# 1. Install latest Python (if using asdf)
asdf install python latest
asdf set python $(asdf latest python) -u

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# 3. Install all dependencies
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements_test.txt

# 4. Copy configuration file
cp mtgjson.properties.example mtgjson5/resources/mtgjson.properties

# 5. Verify setup
python -m mtgjson5 --help
```

### Configuration File
- Copy `mtgjson.properties.example` to `mtgjson5/resources/mtgjson.properties`
- Fill in API keys for external providers (optional for basic functionality)
- The application will fail without this config file present

### Environment Variables
- `MTGJSON5_DEBUG`: Enable debug logging (1 or true)
- `MTGJSON5_OUTPUT_PATH`: Custom output directory (default: project root)
- AWS credentials if using S3 upload functionality

## Essential Commands

### Basic Usage
```bash
# Build specific sets
python -m mtgjson5 --sets NEO VOW MID --pretty

# Build all available sets  
python -m mtgjson5 --all-sets --pretty

# Full build with compiled outputs and compression
python -m mtgjson5 --all-sets --full-build --compress --pretty

# Price build only
python -m mtgjson5 --price-build --pretty
```

### Development & Testing
```bash
# Code formatting and linting
tox -e black-inplace    # Format code
tox -e isort-inplace    # Sort imports
tox -e lint             # Run pylint
tox -e mypy             # Type checking

# Run all quality checks
tox

# Run tests
pytest tests/
```

### Single Test Execution
```bash
# Run specific test file
pytest tests/mtgjson5/test_card_sorting.py -v

# Run specific test method
pytest tests/mtgjson5/test_card_sorting.py::TestClassName::test_method_name -v
```

## Architecture Overview

### Data Flow Pipeline
```
CLI Args → Dispatcher → Providers (fetch) → Builders (transform) → Output Generator (write) → Compression → S3 Upload
```

### Core Modules

**Entry Point & Control Flow:**
- `__main__.py` - CLI entry point and main dispatcher
- `arg_parser.py` - Command line argument parsing
- `mtgjson_config.py` - Configuration management singleton

**Data Providers (External Sources):**
- `ScryfallProvider` - Primary card data from Scryfall API
- `GathererProvider` - Official Wizards Gatherer database
- `WhatsInStandardProvider` - Format legality data
- `GitHubMTGSqliteProvider` - Alternative database formats
- `GitHubDecksProvider` - Pre-constructed deck data

**Data Processing:**
- `set_builder.py` - Constructs individual set objects from provider data
- `price_builder.py` - Aggregates pricing information from multiple sources
- `referral_builder.py` - Creates referral/affiliate link mappings

**Output Generation:**
- `output_generator.py` - Writes JSON files and coordinates compiled outputs
- `compress_generator.py` - Handles file compression for distribution
- `mtgjson_s3_handler.py` - AWS S3 upload functionality

**Data Models:**
- `classes/` - Individual object models (MtgjsonSet, MtgjsonCard, etc.)
- `compiled_classes/` - Aggregated collection models (AllPrintings, AtomicCards, etc.)

### Key Design Patterns

**Provider Pattern:** External data sources are abstracted behind provider interfaces with caching

**Builder Pattern:** Complex objects (sets, cards) are constructed through dedicated builder modules

**Singleton Configuration:** `MtgjsonConfig()` provides centralized access to settings and paths

**Two-Phase Processing:** Some operations like referral building use build + fixup phases

**Format Mapping:** Cards and sets are automatically categorized by format legality (Standard, Modern, etc.)

## Configuration Reference

### Required Files
- `mtgjson5/resources/mtgjson.properties` - Main configuration file with API keys
- Copy from `mtgjson.properties.example` and customize

### Key Environment Variables
- `MTGJSON5_OUTPUT_PATH` - Override default output directory
- `MTGJSON5_DEBUG` - Enable verbose logging
- `AWS_PROFILE` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - For S3 uploads
- `NO_ALERTS` - Disable push notifications

### Command Line Options
- `--sets SET [SET ...]` - Build specific sets by code
- `--all-sets` - Build all available sets
- `--full-build` - Generate compiled outputs (AllPrintings, etc.)
- `--compress` - Compress output files
- `--pretty` - Pretty-print JSON (vs minified)
- `--price-build` - Build only pricing data
- `--resume-build` - Skip sets that already exist in output directory

## Development Patterns

### Adding New Providers
1. Inherit from base provider class
2. Implement data fetching methods
3. Register in appropriate builder modules
4. Add rate limiting if required

### Extending Data Models  
1. Add properties to relevant classes in `classes/`
2. Update `to_json()` methods to include new fields
3. Consider backward compatibility for consumers
4. Update documentation schemas

### Output Format Changes
1. Modify `output_generator.py` for new compiled outputs
2. Update `MtgjsonStructuresObject` for new file paths
3. Consider format-specific variations
4. Test with actual data to ensure JSON validity

### Testing New Features
1. Add unit tests in `tests/mtgjson5/`
2. Use existing patterns for provider mocking
3. Test with small dataset first (`--sets` with 1-2 sets)
4. Validate output JSON structure and content

## Troubleshooting

### Common Issues
- **Missing config file:** Ensure `mtgjson5/resources/mtgjson.properties` exists (copy from `mtgjson.properties.example`)
- **Python version issues:** Use asdf to install and manage Python versions: `asdf list all python | tail -10`
- **Virtual environment not activated:** Always run `source venv/bin/activate` before running commands
- **Missing dependencies:** Run `pip install -r requirements.txt -r requirements_test.txt` if imports fail
- **Memory issues:** Use `--sets` for smaller builds, monitor RAM usage on full builds
- **API rate limits:** Check provider implementations for rate limiting
- **Invalid output:** Use `--pretty` for debugging JSON structure issues

### Performance Optimization
- Provider caching reduces API calls on subsequent runs
- `--resume-build` skips already-built sets
- Gevent enables concurrent processing where applicable
- Consider `MTGJSON5_OUTPUT_PATH` on faster storage for large builds

### Python Version Management
This project uses asdf for Python version management. Key commands:
```bash
# List available Python versions
asdf list all python | grep -E "^[0-9]+\.[0-9]+\.[0-9]+$" | sort -V | tail -10

# Install latest Python
asdf install python latest

# Set Python version globally
asdf set python <version> -u

# Check current Python version
asdf current python
```
