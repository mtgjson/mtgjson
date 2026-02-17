# Polars Pipeline — Contributor Guide

This guide helps developers contribute to the Polars pipeline. It covers where to put code, how to maintain performance, how to test, and what runtime flags exist.

For environment setup (uv, credentials, tox), see the root [`CONTRIBUTING.md`](../CONTRIBUTING.md). For detailed reference on each subsystem, see the other documents in this directory ([README](README.md)).

## Architecture at a Glance

```
┌──────────────────────────┐
│  1. GlobalCache          │  Downloads provider data → LazyFrames
│  data/cache.py        │  (Scryfall, TCGPlayer, CardMarket, etc.)
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  2. PipelineContext      │  Wraps cache, builds derived lookup tables
│  data/context.py      │  consolidate_lookups() → 9 derived tables
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  3. build_cards()        │  Thin orchestrator delegating to stage modules
│  pipeline/core.py     │  9 stage modules in pipeline/stages/
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│  4. Assembly & Output    │  Reads parquet → Pydantic models → JSON/SQLite/CSV/etc.
│  build/assemble.py    │  AssemblyContext, UnifiedOutputWriter
└──────────────────────────┘
```

Most contributions touch one of three areas:
- **New data source** — cache + context + pipeline join
- **New card field / transform** — core + expressions + model
- **Output formatting fix** — assemble + serializers + models

## Where Does My Code Go?

### New external data source

1. **Provider**: Create `providers/{name}/provider.py`
2. **Loading**: Add `{name}_lf` attribute + load method to `data/cache.py`
3. **Dump/reload**: Register in `_dump_and_reload_as_lazy()` so LazyFrame survives the memory optimization step
4. **Context**: Add property to `data/context.py` (with `_test_data` fallback) + `for_testing()` parameter
5. **Pipeline**: Add join step in the appropriate stage module under `pipeline/stages/` (see [stage module reference](pipeline-core.md#stage-module-reference))

See also: [Adding a New Provider](global-cache.md#adding-a-new-provider) in the GlobalCache reference.

### New card field from existing data

1. **Expression** (if purely vectorized): `pipeline/expressions.py`
2. **Transform**: `pipeline/stages/{module}.py` — add function in the relevant [stage module](pipeline-core.md#stage-module-reference), then wire into `pipeline/core.py` orchestrator
3. **Model field**: `models/cards.py` on `CardBase`, `CardAtomicBase`, or `CardPrintingBase`
4. **Sub-model** (if struct/TypedDict): `models/submodels.py`

### Fixing output format or ordering

1. **Assembly logic**: `build/assemble.py`
2. **Serialization**: `build/serializers.py`
3. **Model key ordering**: `models/base.py` (`PolarsMixin.to_polars_dict`)

### New constant, enum, or mapping

→ `consts/` (field definitions, layout types, finish orderings, language codes)

### New output format

→ `build/formats/` (new builder), `build/writer.py` (register), `models/files.py` (file model)

### Price data or new price provider

The price engine is a separate ETL pipeline from the card builder. See [prices.md](prices.md) for the full architecture.

- **New price provider**: `providers/{name}/`, then integrate in `build/price_builder.py` (`build_today_prices_async`)
- **Price ID mapping**: `PriceBuilderContext` in `build/price_builder.py`
- **Price output format**: `build/price_writers.py` (standalone writer functions)
- **Price archive/partition**: `build/price_archive.py`
- **Price S3 sync**: `build/price_s3.py`
- **Referral map**: `build/referral_builder.py`

### Worked example: EDHREC salt score integration

This traces the salt score feature through all layers:

```
providers/salt.py          ← Provider fetches salt data from EDHREC
data/cache.py              ← salt_lf attribute, loaded in load_all()
data/context.py            ← salt_lf property, for_testing(salt_lf=...) param
data/context.py            ← _build_oracle_data_lookup() joins salt by oracleId
pipeline/stages/identifiers.py ← join_oracle_data() brings edhrecSaltiness into pipeline
models/cards.py            ← CardBase.edhrecSaltiness field
```

## Performance Considerations

### Always use LazyFrames until you must collect

```python
# DO — stays lazy, Polars optimizes the entire plan
lf = lf.filter(...).with_columns(...).join(...)

# DON'T — forces materialization, loses optimization opportunities
df = lf.collect()
df = df.filter(...)  # Now in eager DataFrame land unnecessarily
```

### Avoid the use of `map_elements`, `.apply()`, or Python loops on rows

```python
# DON'T — Python UDF, serialization overhead, no Rust optimization
lf = lf.with_columns(
    pl.col("name").map_elements(lambda x: x.replace("AE", "Ae"))
)

# DO — Rust-native batch operation, 10-100x faster
lf = lf.with_columns(
    pl.col("name").str.replace_many(ASCII_OLD, ASCII_NEW)
)
```

See: `expressions.py` → `ascii_name_expr()`

### Use `list.eval()` + `replace_strict()` for ordered list operations

```python
# DON'T — Python UDF on every row
lf = lf.with_columns(
    pl.col("finishes").map_elements(lambda x: sorted(x, key=ORDER.get))
)

# DO — Polars-native list sorting with weight mapping
lf = lf.with_columns(
    pl.col("finishes").list.eval(
        pl.element().sort_by(
            pl.element().replace_strict(WEIGHTS, default=99, return_dtype=pl.Int32)
        )
    )
)
```

See: `expressions.py` → `order_finishes_expr()`

### Use `str.extract_all()` + list ops for parsing

For parsing structured strings like mana costs, use regex extraction and list aggregation instead of Python string parsing.

See: `expressions.py` → `calculate_cmc_expr()`

### Strategic checkpoints prevent query plan explosion

Care must be taken in regards to Polars query optimizer, as the relational complexity and depth of MTG data can (and will) crash the Polars optimization engine.  
To avoid this, we utilize strategically placed collect statements to reset the query plan:

```python
lf = lf.collect().lazy()
```

The pipeline has 3 checkpoints placed in `core.py` between stage groups:
- **Checkpoint 1**: After multi-row joins (identifiers, oracle, set/number, name, cardmarket)
- **Checkpoint 2**: Before relationship operations (otherFaceIds, leadershipSkills, reverseRelated, tokenIds)
- **Checkpoint 3**: Before final enrichment (manual overrides, rebalanced linkage, secret lair subsets, source products)

If your contribution is 'join-heavy', consider adding a checkpoint.

### Always specify `keep` with `.unique()`

```python
# DON'T — arbitrary row selection, non-deterministic results
df = df.unique(subset=["name"])

# DO — deterministic: sort first, then keep="first"
df = df.sort("isPromo", "isOnlineOnly").unique(subset=["name"], keep="first")
```

### Prefer `safe_ops` for defensive operations

The `pipeline/safe_ops.py` module provides null-safe and schema-safe utilities:

```python
from mtgjson5.pipeline.safe_ops import safe_drop, safe_rename, safe_struct_field

# Drop columns that may or may not exist (no crash)
lf = safe_drop(lf, ["temp_col_1", "temp_col_2"])

# Rename with warning if column missing
lf = safe_rename(lf, {"old_name": "new_name"})

# Access struct field with fallback default
expr = safe_struct_field("identifiers", "tcgplayerProductId", default=pl.lit(None))
```

### `orjson.OPT_SORT_KEYS` sorts ALL keys alphabetically

This breaks the expected `{"meta": ..., "data": ...}` ordering in output files. Use streaming JSON writes for structured files instead of whole-file `orjson.dumps()` with `OPT_SORT_KEYS`.

## Testing the Pipeline

### Overview

- Tests live in `tests/mtgjson5/` — run with `pytest tests/mtgjson5/ -v`
- No VCR cassettes needed — tests use `PipelineContext.for_testing()` to bypass GlobalCache
- Legacy tests (`tests/`) are separate and use VCR cassettes

### `PipelineContext.for_testing()` pattern

The core test pattern injects data directly, bypassing all network calls:

```python
ctx = PipelineContext.for_testing(
    cards_lf=pl.LazyFrame({...}),
    sets_lf=pl.LazyFrame({...}),
    meld_triplets={},
    manual_overrides={},
)
```

Full signature:

```python
PipelineContext.for_testing(
    cards_lf=None,          # Raw card data
    sets_lf=None,           # Set metadata
    rulings_lf=None,        # Rulings data
    uuid_cache_lf=None,     # UUID cache
    card_kingdom_lf=None,   # Card Kingdom lookup
    salt_lf=None,           # EDHREC salt data
    orientation_lf=None,    # Orientation data
    meld_triplets=None,     # Meld triplet mappings
    manual_overrides=None,  # Manual overrides dict
    foreigndata_exceptions=None,  # Foreign data exceptions
    resource_path=None,     # Path to resources
    args=None,              # argparse Namespace
    **kwargs,               # Additional test data
)
```

### Shared fixtures (`tests/mtgjson5/conftest.py`)

| Fixture / Helper | Description |
|------------------|-------------|
| `make_card_row(**overrides)` | Dict with sensible defaults for every pipeline column; override only test-relevant fields |
| `make_card_lf(rows)` | Properly-schemaed LazyFrame from a list of `make_card_row()` dicts |
| `make_face_struct(**overrides)` | CardFace struct dict for multi-face card testing |
| `make_meld_triplet_lf()` | 3-card LazyFrame with correct meld columns (BRO / Urza) |
| `make_full_card_df()` | DataFrame with representative column types (String, Int64, Float64, Boolean, List, Struct, null) |
| `simple_ctx` (fixture) | `PipelineContext.for_testing()` with empty meld_triplets and manual_overrides |
| `meld_ctx` (fixture) | `PipelineContext.for_testing()` with sample Urza meld triplet |

### Testing a transform function

```python
def test_my_transform():
    lf = make_card_lf([make_card_row(name="Test Card", manaCost="{2}{W}")])
    result = my_transform(lf).collect()
    assert result["newField"][0] == expected_value
```

### Testing a Polars expression

```python
def test_my_expression():
    df = pl.DataFrame({"col": ["input_value"]})
    result = df.select(my_expr("col").alias("out"))
    assert result["out"][0] == "expected_output"
```

### Testing assembly/output

Use mock `AssemblyContext` patterns from the existing tests in `tests/mtgjson5/test_assembly.py`. Use `make_full_card_df()` for cross-format type coverage.

### Extending `for_testing()` when adding new data

When you add a new LazyFrame to GlobalCache:
1. Add a property to PipelineContext with the `_test_data` fallback pattern
2. Add a parameter to `for_testing()` so tests can inject the data
3. Update the `simple_ctx` and `meld_ctx` fixtures if the data has sensible defaults

See: [Extending for_testing()](pipeline-context.md#extending-for_testing) in the PipelineContext reference.

## Runtime Flags Reference

### Pipeline Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--build-all` | | Shorthand for `--use-models --all-sets --full-build --export all` |
| `--use-models` | `-M` | Use Pydantic model-based assembly |
| `--from-cache` | | Skip pipeline, assemble from cached parquet (requires prior `--use-models` run) |

### Set Selection

| Flag | Short | Description |
|------|-------|-------------|
| `--sets` | `-s` | Specific set(s) to build (Scryfall codes, comma or space separated) |
| `--all-sets` | `-a` | Build all sets |
| `--skip-sets` | `-SS` | Exclude specific sets from the build |

### Output Control

| Flag | Short | Description |
|------|-------|-------------|
| `--full-build` | `-c` | Build prices, MTGSQLive, and compiled outputs |
| `--outputs` | `-O` | Specific compiled outputs (e.g., `AllPrintings,SetList,AtomicCards`) |
| `--export` | `-E` | Export formats: `json`, `sqlite`, `csv`, `parquet`, `psql`, or `all` |
| `--pretty` | `-p` | Pretty-print JSON output |
| `--compress` | `-z` | Compress output files for distribution |

### Performance / Filtering

| Flag | Short | Description |
|------|-------|-------------|
| `--skip-mcm` | | Skip CardMarket data fetching (faster builds) |
| `--parallel` | `-P` | Use parallel compression with ThreadPoolExecutor |
| `--bulk-files` | `-B` | Use Scryfall bulk data files where possible |

### Common Usage Patterns

```bash
# Full build — all sets, all outputs, all export formats
python -m mtgjson5 --build-all

# Single set, individual set file only
python -m mtgjson5 -s MH3

# Single set with compiled outputs
python -m mtgjson5 --build-all -s MH3

# Reassemble from cached parquet (no provider calls)
python -m mtgjson5 --from-cache

# Full build, skip slow CardMarket API, pretty JSON
python -m mtgjson5 --build-all --skip-mcm -p
```

### `--build-all` behavior with `--sets`

- `--build-all` alone → full build (all sets, all compiled outputs, all export formats)
- `--build-all -s MH3` → individual set file + compiled outputs + all exports
- `-s MH3` → individual set file only (no compiled outputs, no exports)

### Environment Variables

| Variable | Description |
|----------|-------------|
| `MTGJSON5_DEBUG=1` | Enable debug logging |
| `MTGJSON5_OUTPUT_PATH=/path` | Override output directory |
| `MTGJSON_OFFLINE_MODE=1` | Force offline testing mode |

## Common Pitfalls

1. **`.unique()` without `keep`** — selects arbitrary rows, causing non-deterministic output. Always specify `keep="first"` with a preceding `.sort()`.

2. **`orjson.OPT_SORT_KEYS`** — sorts ALL keys alphabetically, breaking `{"meta": ..., "data": ...}` ordering. Use streaming JSON writes for structured files.

3. **Forgetting `for_testing()` param** — when adding a new LazyFrame to cache, always add the corresponding `for_testing()` parameter or tests can't inject mock data.

4. **Collecting inside a loop** — `.collect()` in a Python loop destroys Polars' batch processing advantage. Restructure as a single lazy chain with joins or group_by.

5. **Struct field access on null structs** — accessing `.struct.field("x")` on a null struct panics. Use `safe_struct_field()` from `safe_ops.py`.

6. **Not checkpointing after joins** — the query plan grows exponentially with joins. After 3-5 joins, reset with `lf = lf.collect().lazy()`.

7. **TypedDict vs BaseModel wrong choice** — using BaseModel for nested sub-structures (Identifiers, Legalities) costs ~2.5x in parsing performance. Use TypedDict for sub-models, BaseModel only for top-level models. See: [Choosing TypedDict vs BaseModel](models.md#choosing-typeddict-vs-basemodel).

8. **Sealed product UUID joins** — sealed products don't have a top-level `tcgplayerProductId`. Access it via `identifiers.struct.field("tcgplayerProductId")`.

