# Polars Pipeline Reference

This documentation covers the Polars-based pipeline for building MTGJSON data.

## For Contributors

Start with the **[Contributor Guide](contributing.md)** — it covers where to put code, performance rules, testing patterns, runtime flags, and a PR checklist.

## Quick Start

```bash
# Full build (all sets, all outputs, model-based assembly)
python -m mtgjson5 --build-all

# Equivalent explicit flags
python -m mtgjson5 --use-models --all-sets --full-build

# Specific sets only
python -m mtgjson5 -s MH3 BLB

# Decks only
python -m mtgjson5 --outputs Decks
```

## Architecture Overview

The Polars pipeline consists of four main layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    1. DATA LAYER                            │
│  GlobalCache (data/cache.py)                             │
│  - Downloads and caches provider data                       │
│  - Stores as LazyFrames for memory efficiency               │
│  - 10 parallel threads for provider loading                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 2. TRANSFORMATION CONTEXT                   │
│  PipelineContext (data/context.py)                       │
│  - Wraps GlobalCache                                        │
│  - consolidate_lookups() builds derived tables              │
│  - Provides property-based access for testability           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    3. PIPELINE                              │
│  build_cards() (pipeline/core.py → stages/)              │
│  - Thin orchestrator delegates to 9 stage modules           │
│  - Strategic checkpoints, partitioned parquet output        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 4. ASSEMBLY & OUTPUT                        │
│  AssemblyContext (build/context.py)                      │
│  JsonOutputBuilder (build/formats/json.py)               │
│  SQLiteBuilder, CSVBuilder, etc.                            │
│  - Reads from parquet cache                                 │
│  - Writes JSON, SQLite, CSV, Parquet, PostgreSQL            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              5. PRICE ENGINE (separate ETL)                 │
│  PolarsPriceBuilder (build/price_builder.py)             │
│  + price_archive.py, price_writers.py, price_s3.py         │
│  - Fetches daily prices from 5 providers                    │
│  - Date-partitioned parquet data lake with S3 sync          │
│  - Streams AllPrices.json, AllPricesToday.json + SQL        │
└─────────────────────────────────────────────────────────────┘
```

## Documents in This Directory

| Document | Description |
|----------|-------------|
| [contributing.md](contributing.md) | **Contributor guide**: where to put code, performance rules, testing, runtime flags |
| [global-cache.md](global-cache.md) | Data layer: GlobalCache class, provider loading, LazyFrame storage |
| [pipeline-context.md](pipeline-context.md) | PipelineContext class, consolidate_lookups(), derived tables |
| [pipeline-core.md](pipeline-core.md) | Main build_cards() function, transformation stages, checkpoints |
| [assembly-output.md](assembly-output.md) | AssemblyContext, format builders, output generation |
| [models.md](models.md) | Pydantic models: CardSet, CardAtomic, MtgSet, TypedDicts, adapters |
| [prices.md](prices.md) | Price engine: provider fetching, date-partitioned data lake, S3 sync, output |
| [documentation.md](documentation.md) | Pipeline-driven TypeScript types and VitePress documentation generation |

## Data Flow

```
Command Line (--build-all or explicit flags)
        │
        ▼
┌───────────────────┐
│  GlobalCache      │──────────────────────────────────────┐
│  .load_all()      │                                      │
└───────┬───────────┘                                      │
        │ 30+ LazyFrames + resource dicts                  │
        ▼                                                  │
┌───────────────────┐                                      │
│  PipelineContext  │                                      │
│  .from_global_cache()                                    │
└───────┬───────────┘                                      │
        │                                                  │
        ▼                                                  │
┌───────────────────┐                                      │
│  consolidate_     │                                      │
│  lookups()        │                                      │
└───────┬───────────┘                                      │
        │ 6 derived lookup tables                          │
        ▼                                                  │
┌───────────────────┐                                      │
│  build_cards()    │                                      │
│  (pipeline/core)  │                                      │
└───────┬───────────┘                                      │
        │ Partitioned parquet files                        │
        ▼                                                  │
┌───────────────────┐                                      │
│  AssemblyContext  │◄─────────────────────────────────────┘
│  .from_pipeline() │     (also uses GlobalCache data)
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│  Output Writers   │
│  (JSON, SQLite,   │
│   CSV, Parquet,   │
│   PostgreSQL)     │
└───────────────────┘
```

## Key Design Patterns

### 1. Lazy Evaluation with Strategic Checkpoints
Most operations use Polars LazyFrames. At strategic points, the pipeline calls `.collect().lazy()` to reset the query plan and prevent memory explosion.

### 2. Property-Based Context Access
PipelineContext uses properties that check `_test_data` before falling back to GlobalCache. This enables testing without the singleton.

### 3. Vectorized Expressions
Pure Polars expressions in `pipeline/expressions.py` replace Python UDFs for 10-100x performance. See [Performance Rules](contributing.md#performance-rules) for do/don't examples.

### 4. Face-Aware Transformations
`explode_card_faces()` creates separate rows per card face. The `_face_data` struct holds face-specific fields, and `face_field()` coalesces face vs card-level data.

### 5. Partitioned Parquet Output
Cards/tokens are written to `_parquet/setCode={CODE}/0.parquet` for fast set-specific reads during assembly.

### 6. Pydantic Model Hierarchy
Card models use multiple inheritance (CardPrintingFull = CardPrintingBase + CardAtomicBase). TypedDicts are used for nested structures (~2.5x faster than BaseModel).

### 7. PolarsMixin Integration
All models inherit from PolarsMixin, providing `to_polars_dict()`, `polars_schema()`, `to_dataframe()`, and TypeScript generation.

## File Reference

All paths relative to `mtgjson5/`:

| File | Purpose |
|------|---------|
| `pipeline/core.py` | Pipeline orchestrator (delegates to stages/) |
| `pipeline/stages/` | Stage modules: explode, basic_fields, identifiers, legalities, relationships, derived, signatures, metadata, output |
| `pipeline/expressions.py` | Vectorized Polars expressions |
| `data/cache.py` | Data layer, provider loading |
| `data/context.py` | Transformation context |
| `build/assemble.py` | Assembly utilities |
| `build/context.py` | Assembly configuration |
| `build/writer.py` | Format dispatch |
| `build/price_builder.py` | Price engine orchestrator + context |
| `build/price_archive.py` | Price archive: load, save, merge, prune, partition |
| `build/price_writers.py` | Price output: JSON streaming, SQLite, SQL, CSV |
| `build/price_s3.py` | Price S3: sync operations |
| `build/referral_builder.py` | Referral map generation for purchase URLs |
| `models/cards.py` | Card Pydantic models |
| `models/sets.py` | Set Pydantic models |
| `models/submodels.py` | TypedDict sub-models |
| `models/base.py` | PolarsMixin, file base classes |
| `compress_generator.py` | Output compression (gzip, xz, bz2, zip) |
