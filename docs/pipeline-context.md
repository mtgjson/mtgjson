# PipelineContext - Transformation Context

**File**: `mtgjson5/v2/data/context.py`

PipelineContext wraps GlobalCache and builds derived lookup tables used throughout the transformation pipeline. It acts as a bridge between raw cached data and the `build_cards()` function.

## Overview

```python
from mtgjson5.v2.data.context import PipelineContext

# Production: create from GlobalCache
ctx = PipelineContext.from_global_cache(args)
ctx.consolidate_lookups()

# Testing: create with explicit test data
ctx = PipelineContext.for_testing(
    cards_lf=test_cards,
    sets_lf=test_sets,
)
```

## Class Structure

```python
@dataclass
class PipelineContext:
    # Internal state
    _cache: GlobalCache | None    # Reference to singleton
    _test_data: dict[str, Any]    # Test data override

    # Core configuration
    args: Namespace | None        # CLI arguments
    scryfall_id_filter: set[str] | None  # Optional Scryfall ID filter

    # Derived lookups (built by consolidate_lookups)
    identifiers_lf: pl.LazyFrame | None
    oracle_data_lf: pl.LazyFrame | None
    set_number_lf: pl.LazyFrame | None
    name_lf: pl.LazyFrame | None
    signatures_lf: pl.LazyFrame | None
    watermark_overrides_lf: pl.LazyFrame | None
    face_foreign_lf: pl.LazyFrame | None
    final_cards_lf: pl.LazyFrame | None

    # Materialized data
    uuid_lookup_df: pl.DataFrame | None
    face_flavor_names_df: pl.DataFrame | None

    # Model types
    card_set_model: type          # Default: CardSet
    card_token_model: type        # Default: CardToken
    card_deck_model: type         # Default: CardDeck
    card_atomic_model: type       # Default: CardAtomic

    # Additional configuration
    categoricals: DynamicCategoricals | None
    resource_path: Path | None
    mcm_set_map: dict[str, dict[str, Any]]
```

## Property-Based Access

PipelineContext exposes GlobalCache data through properties. Properties check `_test_data` first, enabling dependency injection for tests.

```python
@property
def cards_lf(self) -> pl.LazyFrame:
    if self._test_data and "cards_lf" in self._test_data:
        return self._test_data["cards_lf"]
    return self._cache.cards_lf

@property
def sets_lf(self) -> pl.LazyFrame:
    # Same pattern...
```

### Available Properties

| Property | Source | Description |
|----------|--------|-------------|
| `cards_lf` | GlobalCache | Raw Scryfall card data |
| `rulings_lf` | GlobalCache | Card rulings |
| `sets_lf` | GlobalCache | Set metadata |
| `card_kingdom_lf` | GlobalCache | Card Kingdom inventory |
| `mcm_lookup_lf` | GlobalCache | CardMarket identifiers |
| `salt_lf` | GlobalCache | EDHREC salt scores |
| `sealed_products_lf` | GlobalCache | Sealed product data |
| `decks_lf` | GlobalCache | Decklists |
| `boosters_lf` | GlobalCache | Booster configurations |
| `tcg_skus_lf` | GlobalCache | TCGPlayer SKUs |
| `orientation_lf` | GlobalCache | Card image orientations |
| `gatherer_lf` | GlobalCache | Gatherer page data |

## Factory Methods

### `from_global_cache(args)`

Production factory that creates context from the loaded GlobalCache singleton.

```python
@classmethod
def from_global_cache(cls, args: Namespace | None = None) -> PipelineContext:
    from mtgjson5.v2.data.cache import GLOBAL_CACHE
    from mtgjson5.v2.utils import discover_categoricals

    # Discover categoricals from the raw cards data
    categoricals = None
    if GLOBAL_CACHE.cards_lf is not None:
        categoricals = discover_categoricals(
            GLOBAL_CACHE.cards_lf,
            GLOBAL_CACHE.sets_lf,
        )

    return cls(
        _cache=GLOBAL_CACHE,
        args=args,
        categoricals=categoricals,
        scryfall_id_filter=GLOBAL_CACHE._scryfall_id_filter,
        resource_path=constants.RESOURCE_PATH,
    )
```

### `for_testing()`

Test factory that bypasses GlobalCache entirely.

```python
@classmethod
def for_testing(
    cls,
    cards_lf: pl.LazyFrame | None = None,
    sets_lf: pl.LazyFrame | None = None,
    rulings_lf: pl.LazyFrame | None = None,
    # ... more optional parameters
) -> PipelineContext:
    """Create context with explicit test data."""

    test_data = {}
    if cards_lf is not None:
        test_data["cards_lf"] = cards_lf
    # ... store all provided data

    return cls(
        args=Namespace(),
        _cache=None,
        _test_data=test_data,
    )
```

## consolidate_lookups()

The main method that builds derived lookup tables from raw cached data. Called after context creation, before `build_cards()`.

```python
def consolidate_lookups(self) -> PipelineContext:
    """Build 9 derived lookup tables via lazy joins."""

    self._build_identifiers_lookup()
    self._build_oracle_data_lookup()
    self._build_set_number_lookup()
    self._build_name_lookup()
    self._build_signatures_lookup()
    self._build_watermark_overrides_lookup()
    self._load_face_flavor_names()
    self._build_mcm_set_map()
    self._build_mcm_lookup()

    return self
```

### 1. Identifiers Lookup

**Method**: `_build_identifiers_lookup()`
**Output**: `self.identifiers_lf`
**Key**: `(scryfallId, side)`

Joins UUID cache with Card Kingdom data to provide identifiers for each card face.

```python
# Schema
identifiers_lf = pl.LazyFrame({
    "scryfallId": str,
    "side": str | None,          # "a", "b", "c", or None
    "cachedUuid": str,           # Pre-computed UUID from cache
    "cardKingdomId": str | None,
    "cardKingdomEtchedId": str | None,
    "cardKingdomFoilId": str | None,
    "orientation": str | None,   # "normal", "flip", etc.
})
```

**Join Strategy**: FULL join to include Card Kingdom-only cards

### 2. Oracle Data Lookup

**Method**: `_build_oracle_data_lookup()`
**Output**: `self.oracle_data_lf`
**Key**: `oracleId`

Aggregates per-oracle data: salt scores, rulings, and printings list.

```python
# Schema
oracle_data_lf = pl.LazyFrame({
    "oracleId": str,
    "edhrecSaltiness": float | None,
    "edhrecRank": int | None,
    "rulings": list[struct],     # [{date, text}]
    "printings": list[str],      # [setCode1, setCode2, ...]
})
```

**Join Strategy**: FULL join salt, rulings, and cards aggregated by oracleId

### 3. Set/Number Lookup

**Method**: `_build_set_number_lookup()`
**Output**: `self.set_number_lf`
**Key**: `(setCode, number)`

The most complex lookup - handles foreign data, duel deck sides, and UUID generation.

```python
# Schema
set_number_lf = pl.LazyFrame({
    "setCode": str,
    "number": str,               # Collector number
    "foreignData": list[struct], # [{language, name, text, ...}]
    "duelDeck": str | None,      # "a" or "b"
    "foreignUuids": list[str],   # UUIDs for foreign cards
})
```

**Sub-lookups built**:
1. Foreign data aggregation (non-English cards grouped by setCode+number)
2. UUID generation for foreign cards
3. Duel deck side detection
4. Foreign data exceptions from resources

### 4. Name Lookup

**Method**: `_build_name_lookup()`
**Output**: `self.name_lf`
**Key**: `name`

Provides meld card parts and leadership skills by card name.

```python
# Schema
name_lf = pl.LazyFrame({
    "name": str,
    "cardParts": list[str] | None,      # Meld parts
    "leadershipSkills": struct | None,  # Commander attributes
})
```

### 5. Signatures Lookup

**Method**: `_build_signatures_lookup()`
**Output**: `self.signatures_lf`
**Key**: `scryfallId`

Special signature data for signed cards.

```python
# Schema
signatures_lf = pl.LazyFrame({
    "scryfallId": str,
    "signatures": list[str],
})
```

### 6. Watermark Overrides Lookup

**Method**: `_build_watermark_overrides_lookup()`
**Output**: `self.watermark_overrides_lf`
**Key**: `scryfallId`

Manual watermark fixes from resources.

```python
# Schema
watermark_overrides_lf = pl.LazyFrame({
    "scryfallId": str,
    "watermark": str,
})
```

### 7. Face Flavor Names

**Method**: `_load_face_flavor_names()`
**Output**: `self.face_flavor_names_df`

Loads face-specific flavor names for multi-face cards.

### 8. MCM Set Map

**Method**: `_build_mcm_set_map()`
**Output**: `self.mcm_set_map`

Builds a mapping of set codes to CardMarket set metadata.

### 9. MCM Lookup

**Method**: `_build_mcm_lookup()`

Builds CardMarket identifier lookups for card-level MCM ID joins.

## Usage in Pipeline

The derived lookups are used in `build_cards()` join operations:

```python
# In pipeline/core.py

def join_identifiers(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    return lf.join(
        ctx.identifiers_lf,
        on=["scryfallId", "side"],
        how="left"
    )

def join_oracle_data(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    return lf.join(
        ctx.oracle_data_lf,
        on="oracleId",
        how="left"
    )

def join_set_number_data(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    return lf.join(
        ctx.set_number_lf,
        on=["setCode", "number"],
        how="left"
    )
```

## Lifecycle

```
1. GlobalCache.load_all()
   └── Downloads and caches all provider data

2. PipelineContext.from_global_cache(args)
   └── Wraps cache, discovers categoricals

3. ctx.consolidate_lookups()
   └── Builds 9 derived lookup tables

4. build_cards(ctx)
   └── Uses lookups for joins during transformation

5. AssemblyContext.from_pipeline(ctx)
   └── Reads parquet output, uses some ctx data
```

## Memory Efficiency

The context stores LazyFrames for all lookups. They only materialize when:

1. Joined in the pipeline (via `.collect()`)
2. Checkpoints in `build_cards()` force collection

The only pre-materialized data:
- `uuid_lookup_df` - Needed for reverse UUID lookups
- `face_flavor_names_df` - Small dataset

## Testing Patterns

### Unit Test with Minimal Data

```python
def test_card_transformation():
    ctx = PipelineContext.for_testing(
        cards_lf=pl.LazyFrame({
            "id": ["abc123"],
            "name": ["Lightning Bolt"],
            "set": ["lea"],
            # ... minimal fields
        }),
        sets_lf=pl.LazyFrame({
            "code": ["lea"],
            "name": ["Limited Edition Alpha"],
        }),
    )

    # Test specific transformation
    result = some_transform(ctx.cards_lf, ctx)
    assert result.collect()["name"][0] == "Lightning Bolt"
```

### Integration Test with Cassettes

```python
@pytest.mark.vcr
def test_full_pipeline():
    # Uses VCR cassettes for HTTP calls
    cache = GlobalCache()
    cache.load_all(set_codes=["LEA"])

    ctx = PipelineContext.from_global_cache(args)
    ctx.consolidate_lookups()

    build_cards(ctx)

    # Assert on parquet output
    result = pl.read_parquet("_parquet/setCode=LEA/0.parquet")
    assert len(result) > 0
```

## Extending for_testing()

When you add a new LazyFrame to GlobalCache, you must also update `for_testing()` so tests can inject mock data for the new source.

### Steps

1. **Add a property** to PipelineContext with the `_test_data` fallback:
   ```python
   @property
   def new_data_lf(self) -> pl.LazyFrame:
       if self._test_data and "new_data_lf" in self._test_data:
           return self._test_data["new_data_lf"]
       return self._cache.new_data_lf
   ```

2. **Add a parameter** to `for_testing()`:
   ```python
   @classmethod
   def for_testing(
       cls,
       ...,
       new_data_lf: pl.LazyFrame | None = None,  # ← add here
       ...
   ) -> PipelineContext:
   ```
   Store it in the `_test_data` dict with the key `"new_data_lf"`.

3. **Update test fixtures** — if the data has useful defaults (e.g., an empty LazyFrame with the correct schema), update `simple_ctx` and `meld_ctx` in `tests/mtgjson5/v2/conftest.py`.

### Why this matters

Without the `for_testing()` parameter, tests that exercise code paths touching the new data will crash with `AttributeError` on the missing GlobalCache (which is `None` in test mode).
