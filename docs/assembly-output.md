# Assembly and Output

This document covers how pipeline output (parquet files) is assembled into final MTGJSON formats.

## Overview

```
Pipeline Output (parquet)
        │
        ▼
┌─────────────────────┐
│  AssemblyContext    │  ← build/context.py
│  - Loads parquet    │
│  - Merges set meta  │
│  - Prepares decks   │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  Output Writers     │  ← build/writer.py
│  - Routes to format │
│  - Coordinates jobs │
└─────────┬───────────┘
          │
    ┌─────┴─────┬─────────┬──────────┬───────────┐
    ▼           ▼         ▼          ▼           ▼
┌───────┐  ┌────────┐  ┌─────┐  ┌─────────┐  ┌──────────┐
│ JSON  │  │ SQLite │  │ CSV │  │ Parquet │  │ Postgres │
└───────┘  └────────┘  └─────┘  └─────────┘  └──────────┘
```

## AssemblyContext

**File**: `mtgjson5/v2/build/context.py`

Holds configuration and data needed for all output builders.

### Creation

```python
from mtgjson5.v2.build.context import AssemblyContext

# From pipeline (after build_cards)
assembly_ctx = AssemblyContext.from_pipeline(pipeline_ctx)

# From cache (skip pipeline, use cached files)
assembly_ctx = AssemblyContext.from_cache()
```

### Structure

```python
@dataclass
class AssemblyContext:
    parquet_dir: pathlib.Path      # _parquet/setCode={code}/*.parquet
    tokens_dir: pathlib.Path       # _parquet_tokens/setCode={code}/*.parquet
    set_meta: dict[str, dict]      # Merged set metadata
    meta: dict[str, str]           # {date, version}
    decks_df: pl.DataFrame | None  # GitHub decks
    sealed_df: pl.DataFrame | None # Sealed products
    booster_configs: dict          # Booster pack definitions
    token_products: dict[str, list]  # Token product mappings
    output_path: pathlib.Path      # Output root
    pretty: bool                   # Pretty-print JSON
    # Scryfall catalog data
    keyword_data: dict[str, list[str]]     # Keyword categories
    card_type_data: dict[str, list[str]]   # Card type categories
    super_types: list[str]                 # Supertypes list
    planar_types: list[str]                # Planar types list
```

### `from_pipeline()`

Builds context from PipelineContext after `build_cards()`:

1. **Load and merge set metadata**
   - Base metadata from Scryfall
   - Compute `totalSetSize` from parquet row counts
   - Apply translations from resources
   - Apply TCG overrides
   - Add keyrune codes for icons

2. **Detect token set codes**
   - Format: T{code} or {code}
   - Use `tokenSetCode` from metadata if available

3. **Load decks and sealed products**
   - From GlobalCache via pipeline context

4. **Create meta object**
   - Build date (today)
   - MTGJSON version

### `from_cache()`

Fast path that loads from cached files (skips pipeline):

```python
# Required cached files:
# CACHE_PATH/_assembly_set_meta.json
# CACHE_PATH/_assembly_boosters.json
# CACHE_PATH/_assembly_decks.parquet
# CACHE_PATH/_assembly_sealed.parquet
```

## Pipeline Bridge

**Note**: The bridge functionality is integrated into the build system rather than existing as a separate module.

### `assemble_with_models()`

Primary assembly function using Pydantic models:

```python
def assemble_with_models(
    ctx: PipelineContext,
    streaming: bool = True,
    outputs: set[str] | None = None,
    pretty: bool = False,
) -> dict[str, int]:
    """
    Returns: {filename: record_count}
    """
```

**Process**:
1. Create AssemblyContext from PipelineContext
2. Build JsonOutputBuilder
3. Write outputs:
   - `AllPrintings.json` (streaming for large files)
   - `AtomicCards.json`
   - `SetList.json`
   - Individual set files (parallel)
   - Deck files + `DeckList.json`

### `assemble_json_outputs()`

Legacy dict-based assembly (no Pydantic models):

```python
def assemble_json_outputs(
    ctx: PipelineContext,
    include_referrals: bool = False,
    parallel: bool = True,
    max_workers: int = 30,
    pretty: bool = False,
) -> dict[str, int]:
```

## Assembler Classes

**File**: `mtgjson5/v2/build/assemble.py`

### Base Assembler

```python
class Assembler:
    """Load from parquet cache."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def load_set_cards(self, code: str) -> pl.DataFrame:
        """Load cards for a specific set."""
        path = self.ctx.parquet_dir / f"setCode={code}" / "0.parquet"
        return pl.read_parquet(path)

    def load_set_tokens(self, code: str) -> pl.DataFrame:
        """Load tokens for a specific set."""
        path = self.ctx.tokens_dir / f"setCode={code}" / "0.parquet"
        return pl.read_parquet(path)

    def load_all_cards(self) -> pl.LazyFrame:
        """Load all cards as LazyFrame."""
        return pl.scan_parquet(self.ctx.parquet_dir / "**/*.parquet")

    def iter_set_codes(self) -> list[str]:
        """List all set codes with parquet files."""
        return [p.name.split("=")[1] for p in self.ctx.parquet_dir.iterdir()]
```

### AtomicCardsAssembler

Groups cards by name for `AtomicCards.json`:

```python
class AtomicCardsAssembler(Assembler):
    def iter_atomic(self) -> Iterator[tuple[str, list[dict]]]:
        """
        Yields (card_name, [card_dict, card_dict, ...])
        Grouped by name, sorted alphabetically.
        """
```

### SetAssembler

Builds complete set objects:

```python
class SetAssembler(Assembler):
    def build(self, code: str) -> dict:
        """
        Build CardSet object for a set code.

        Returns:
        {
            "name": "Modern Horizons 3",
            "code": "mh3",
            "cards": [...],
            "tokens": [...],
            "booster": {...},
            # ... metadata
        }
        """
```

### DeckAssembler

Expands deck card lists with full card data:

```python
class DeckAssembler:
    def expand_card_list(self, col: str) -> list[CardDeck]:
        """
        Expand UUID references to full card objects.

        Input: ["uuid1", "uuid2", "uuid1"]
        Output: [CardDeck(...), CardDeck(...), CardDeck(...)]
        """
```

### SetListAssembler

Builds `SetList.json` metadata:

```python
class SetListAssembler:
    def build(self) -> list[dict]:
        """
        Returns list of set metadata (no cards):
        [
            {"name": "Modern Horizons 3", "code": "mh3", ...},
            ...
        ]
        """
```

### DeckListAssembler

Builds `DeckList.json` metadata:

```python
class DeckListAssembler:
    def build(self) -> list[dict]:
        """
        Returns list of deck metadata (no cards):
        [
            {"name": "Deck Name", "code": "deck_code", ...},
            ...
        ]
        """
```

### TcgplayerSkusAssembler

Builds `TcgplayerSkus.json` with TCGPlayer SKU data:

```python
class TcgplayerSkusAssembler(Assembler):
    def build(self) -> dict[str, list[dict]]:
        """Returns UUID -> [SKU objects] mapping."""
```

### TableAssembler

Assembles flattened table representations for relational formats:

```python
class TableAssembler(Assembler):
    def build(self) -> dict[str, pl.DataFrame]:
        """Returns dict of table_name -> DataFrame for relational output."""
```

### KeywordsAssembler

Assembles `Keywords.json`:

```python
class KeywordsAssembler:
    def build(self) -> dict:
        """Returns keyword data (abilityWords, keywordAbilities, keywordActions)."""
```

### CardTypesAssembler

Assembles `CardTypes.json`:

```python
class CardTypesAssembler:
    def build(self) -> dict:
        """Returns card type data grouped by category."""
```

### AllIdentifiersAssembler

Assembles `AllIdentifiers.json` (UUID -> card mapping):

```python
class AllIdentifiersAssembler(Assembler):
    def build(self) -> dict[str, dict]:
        """Returns UUID -> CardSet dict mapping."""
```

### EnumValuesAssembler

Assembles `EnumValues.json`:

```python
class EnumValuesAssembler:
    def build(self) -> dict[str, dict[str, list[str]]]:
        """Returns enum value data grouped by model and field."""
```

### CompiledListAssembler

Assembles compiled list files (e.g., `CardTypes.json` lists):

```python
class CompiledListAssembler:
    def build(self) -> list[str]:
        """Returns a sorted list of compiled values."""
```

## Output Writer

**File**: `mtgjson5/v2/build/writer.py`

### UnifiedOutputWriter

Routes to format-specific builders:

```python
class UnifiedOutputWriter:
    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def write(self, format_type: str) -> Path | None:
        """
        Dispatch to format builder:
        - "json" -> JsonOutputBuilder
        - "sqlite" -> SQLiteBuilder
        - "csv" -> CSVBuilder
        - "parquet" -> ParquetBuilder
        - "sql" -> SQLiteBuilder.write_text_dump()
        - "psql" -> PostgresBuilder
        """

    def write_all(self, formats: list[str] | None = None) -> dict[str, Path]:
        """Write multiple formats."""
```

## Format Builders

### JSON (`v2/build/formats/json.py`)

```python
class JsonOutputBuilder:
    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def write_all_printings(self, streaming: bool = True) -> Path:
        """
        Write AllPrintings.json

        If streaming=True:
        - Writes incrementally to avoid memory issues
        - Iterates sets one at a time

        Structure:
        {
            "meta": {...},
            "data": {
                "MH3": {...},
                "BLB": {...},
                ...
            }
        }
        """

    def write_atomic_cards(self) -> Path:
        """
        Write AtomicCards.json

        Structure:
        {
            "meta": {...},
            "data": {
                "Lightning Bolt": [...],
                "Counterspell": [...],
                ...
            }
        }
        """

    def write_set_list(self) -> Path:
        """Write SetList.json (set metadata only)."""

    def write_individual_sets(self, parallel: bool = True) -> int:
        """
        Write individual set files: MH3.json, BLB.json, etc.
        Uses ThreadPoolExecutor if parallel=True.
        """

    def write_decks(self) -> int:
        """Write individual deck files + DeckList.json."""

    def write_all(self) -> dict[str, int]:
        """Write all JSON outputs."""
```

### SQLite (`v2/build/formats/sqlite.py`)

```python
class SQLiteBuilder:
    def write(self) -> Path:
        """
        Write AllPrintings.sqlite

        Tables:
        - cards (flattened card data)
        - tokens
        - sets
        - set_translations
        - set_booster_*
        - meta

        All columns are TEXT type.
        Indexes on: uuid, name, setCode
        """

    def write_text_dump(self) -> Path:
        """
        Write AllPrintings.sql

        SQL INSERT statements for importing into other databases.
        """
```

**Flattening**: Nested columns are flattened:
- `identifiers.scryfallId` → `scryfallId`
- `legalities.standard` → `legalities_standard`
- `foreignData[0].name` → separate `foreign_data` table

### CSV (`v2/build/formats/csv.py`)

```python
class CSVBuilder:
    def write(self) -> Path:
        """
        Write normalized CSV tables:

        cards.csv           - Main card data
        tokens.csv          - Token data
        sets.csv            - Set metadata
        set_translations.csv
        card_identifiers.csv
        card_legalities.csv
        card_rulings.csv
        card_foreign_data.csv
        meta.csv
        """
```

### Parquet (`v2/build/formats/parquet.py`)

```python
class ParquetBuilder:
    def write(self) -> Path:
        """
        Write Parquet files:

        AllPrintings.parquet - Full nested structure (like JSON)

        Normalized tables:
        - cards.parquet
        - tokens.parquet
        - sets.parquet
        - identifiers.parquet
        - legalities.parquet
        - rulings.parquet
        - foreign_data.parquet
        - booster_*.parquet
        - meta.parquet
        """
```

### PostgreSQL (`v2/build/formats/postgres.py`)

```python
class PostgresBuilder:
    def write(self) -> Path:
        """
        Write PostgreSQL-compatible output:

        If ADBC connection configured:
        - Direct write to database

        Otherwise:
        - COPY format dump file
        - Can be imported via: psql -f file.sql
        """
```

### MySQL (`v2/build/formats/mysql.py`)

```python
class MySQLBuilder:
    def write(self) -> Path:
        """
        Write MySQL-compatible output:

        Generates MySQL INSERT statements and schema definitions.
        """
```

## Output File Structure

```
output_path/
├── AllPrintings.json           # All sets with cards
├── AllPrintings.json.gz        # Compressed
├── AllPrintings.json.xz        # XZ compressed
├── AllPrintings.sqlite         # SQLite database
├── AllPrintings.sql            # SQL dump
├── AllPrintings.parquet        # Parquet (nested)
│
├── AtomicCards.json            # Cards grouped by name
├── SetList.json                # Set metadata only
│
├── MH3.json                    # Individual set
├── BLB.json
├── ...
│
├── decks/
│   ├── DeckList.json           # Deck metadata
│   ├── Deck_Name_1.json
│   ├── Deck_Name_2.json
│   └── ...
│
├── csv/                        # CSV tables
│   ├── cards.csv
│   ├── tokens.csv
│   ├── sets.csv
│   └── ...
│
└── parquet/                    # Parquet tables
    ├── cards.parquet
    ├── tokens.parquet
    └── ...
```

## Streaming JSON

For `AllPrintings.json` (can be 1GB+), streaming write avoids memory issues:

```python
def write_all_printings_streaming(self) -> Path:
    with open(path, "w") as f:
        f.write('{"meta":')
        json.dump(self.ctx.meta, f)
        f.write(',"data":{')

        for i, code in enumerate(set_codes):
            if i > 0:
                f.write(",")

            # Load and write one set at a time
            set_data = self.assembler.build(code)
            f.write(f'"{code}":')
            json.dump(set_data, f)

        f.write("}}")
```

## Parallel Set Writing

Individual set files are written in parallel:

```python
def write_individual_sets(self, parallel: bool = True) -> int:
    if parallel:
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = [
                executor.submit(self._write_set, code)
                for code in set_codes
            ]
            for future in as_completed(futures):
                future.result()  # Raise any exceptions
    else:
        for code in set_codes:
            self._write_set(code)
```

## Usage Examples

### Full Build

```python
# After build_cards(ctx)
assembly_ctx = AssemblyContext.from_pipeline(ctx)
writer = UnifiedOutputWriter(assembly_ctx)
writer.write_all(["json", "sqlite", "csv", "parquet"])
```

### JSON Only

```python
assembly_ctx = AssemblyContext.from_pipeline(ctx)
builder = JsonOutputBuilder(assembly_ctx)
builder.write_all()
```

### Single Set

```python
assembly_ctx = AssemblyContext.from_pipeline(ctx)
assembler = SetAssembler(assembly_ctx)
mh3_data = assembler.build("MH3")
```

### From Cache (Skip Pipeline)

```python
# Requires previously cached assembly files
assembly_ctx = AssemblyContext.from_cache()
if assembly_ctx:
    writer = UnifiedOutputWriter(assembly_ctx)
    writer.write("json")
```
