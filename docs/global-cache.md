# GlobalCache - Data Layer

**File**: `mtgjson5/v2/data/cache.py`

The GlobalCache is a singleton that downloads, caches, and stores all provider data as Polars LazyFrames. It serves as the foundation for the entire Polars pipeline.

## Overview

```python
from mtgjson5.v2.data.cache import GlobalCache

# Singleton access
cache = GlobalCache()

# Load all data (called from __main__.py dispatcher)
cache.load_all(
    set_codes=["MH3", "BLB"],      # Optional: filter to specific sets
    output_types={"AllPrintings"}, # Optional: output type filter
    export_formats=None,            # Optional: export format filter
    skip_mcm=False                  # Skip CardMarket loading
)
```

## LazyFrame Storage

All bulk data is stored as LazyFrames (suffix `_lf`) for memory efficiency. LazyFrames defer computation until explicitly collected.

### Bulk Data LazyFrames

| Attribute | Source | Description |
|-----------|--------|-------------|
| `cards_lf` | Scryfall bulk | Raw card data from default-cards.json |
| `rulings_lf` | Scryfall bulk | Card rulings |
| `sets_lf` | Scryfall API | Set metadata |

### Provider Data LazyFrames

| Attribute | Source | Description |
|-----------|--------|-------------|
| `card_kingdom_lf` | Card Kingdom | Inventory/pricing data |
| `card_kingdom_raw_lf` | Card Kingdom | Raw inventory data (pre-normalization) |
| `mcm_lookup_lf` | CardMarket | EU market identifiers |
| `salt_lf` | EDHREC | Commander saltiness scores |
| `spellbook_lf` | Commander Spellbook | Combo/synergy data |
| `meld_lookup_lf` | Resources | Meld card part lookups |
| `sld_subsets_lf` | MtgWiki | Secret Lair subset data |
| `tcg_skus_lf` | TCGPlayer | SKU mappings |
| `gatherer_lf` | Gatherer pages | Wizards database IDs |
| `orientation_lf` | Scryfall images | Card image orientations |
| `multiverse_bridge_lf` | Wizards | Multiverse ID bridge data |

### Sealed/Deck LazyFrames

| Attribute | Source | Description |
|-----------|--------|-------------|
| `sealed_products_lf` | GitHub sealed-products | Booster boxes, precons |
| `sealed_cards_lf` | GitHub sealed-products | Card-to-product mappings |
| `sealed_contents_lf` | GitHub sealed-products | Sealed product contents |
| `decks_lf` | GitHub mtgjson-decks | Decklists |
| `boosters_lf` | Scryfall sets | Booster configurations |
| `token_products_lf` | GitHub sealed-products | Token product mappings |

### Marketplace Mapping LazyFrames

| Attribute | Source | Description |
|-----------|--------|-------------|
| `tcg_sku_map_lf` | TCGPlayer | TCGPlayer SKU mapping |
| `tcg_to_uuid_lf` | Computed | TCGPlayer product ID → UUID bridge |
| `tcg_etched_to_uuid_lf` | Computed | TCGPlayer etched product ID → UUID bridge |
| `mtgo_to_uuid_lf` | Computed | MTGO ID → UUID bridge |
| `scryfall_to_uuid_lf` | Computed | Scryfall ID → UUID bridge |
| `cardmarket_to_uuid_lf` | Computed | CardMarket ID → UUID bridge |
| `uuid_to_oracle_lf` | Computed | UUID → Oracle ID bridge |

### Aggregation LazyFrames

| Attribute | Built From | Description |
|-----------|------------|-------------|
| `oracle_lookup_lf` | cards + salt + rulings | Per-oracleId aggregations |
| `uuid_cache_lf` | Previous builds | UUID -> identifier mappings |
| `foreign_data_lf` | cards_lf | Non-English card data |
| `languages_lf` | cards_lf | Available languages per card |
| `final_cards_lf` | Pipeline output | Final processed card data |

## Resource Dictionaries

Some data is stored as Python dicts rather than LazyFrames:

| Attribute | Source | Description |
|-----------|--------|-------------|
| `meld_data` | resources/meld.json | Meld triplet definitions |
| `meld_triplets` | Computed | {card_name -> [part_a, part_b, result]} |
| `meld_overrides` | resources/meld_overrides.json | Manual UUID fixes |
| `manual_overrides` | resources/manual_overrides.json | Card-specific fixes |
| `gatherer_map` | resources/gatherer_map.json | Scryfall ID -> Gatherer ID |
| `set_translations` | resources/translations/*.json | Localized set names |
| `base_set_sizes` | resources/base_set_sizes.json | Custom set sizes |

## Key Methods

### `load_all()`

Main entry point that orchestrates parallel loading of all data sources.

```python
def load_all(
    self,
    set_codes: list[str] | None = None,
    output_types: set[str] | None = None,
    export_formats: set[str] | None = None,
    skip_mcm: bool = False
) -> GlobalCache:
```

**Loading Sequence**:

1. **Download bulk data** (Scryfall default-cards.json, rulings.json)
2. **Load bulk data** into DataFrames
3. **Load resource JSON files** (meld, overrides, translations)
4. **Load set metadata** from Scryfall API
5. **Parallel provider loading** (10 threads):
   - Card Kingdom inventory
   - EDHREC salt scores
   - Commander Spellbook
   - Gatherer page data
   - What's in Standard
   - GitHub sealed products
   - Secret Lair subsets
   - (Optional) CardMarket data
6. **Apply dynamic categoricals** for memory optimization
7. **Dump to parquet and reload as lazy** (memory optimization)

### `_dump_and_reload_as_lazy()`

Critical memory optimization that serializes all DataFrames to parquet files and reloads them as LazyFrames.

```python
def _dump_and_reload_as_lazy(self) -> None:
    """
    Serialize all DataFrames to CACHE_PATH/lazy/*.parquet
    then reload as LazyFrames to defer computation.
    """
```

**Why this matters**:
- Raw DataFrames can consume gigabytes of memory
- LazyFrames only load data when collected
- Parquet provides efficient columnar storage
- Subsequent loads are faster from local parquet

### Scryfall ID Filtering

For deck-only builds, the cache can filter to only cards needed for decks:

```python
def _get_deck_scryfall_ids(self) -> set[str]:
    """
    Extract all UUIDs from decks, then reverse-lookup scryfallIds.
    Returns set of scryfallIds needed for deck building.
    """
```

## Caching Strategy

### File Locations

```
CACHE_PATH/
├── default-cards.json       # Scryfall bulk download
├── rulings.json             # Scryfall rulings
├── lazy/                    # Parquet cache
│   ├── cards.parquet
│   ├── rulings.parquet
│   ├── sets.parquet
│   ├── card_kingdom.parquet
│   ├── mcm_lookup.parquet
│   └── ...
└── _parquet/                # Pipeline output (after build_cards)
    └── setCode=MH3/
        └── 0.parquet
```

### Cache Invalidation

The cache does not automatically invalidate. To force fresh data:

```bash
# Delete the cache directory
rm -rf ~/.mtgjson5_cache/

# Or delete specific provider files
rm ~/.mtgjson5_cache/lazy/card_kingdom.parquet
```

## Provider Loading Details

### Scryfall (Primary Source)

```python
# Bulk data download (default-cards.json ~200MB)
ScryfallProvider.download_all_bulk_data()

# Set metadata (API calls)
ScryfallProvider.get_all_sets()
```

### Card Kingdom

```python
# Loads inventory CSV with columns:
# - ck_id, name, set_name, collector_number, foil, price
CardKingdomProvider.get_inventory()
```

### EDHREC Salt

```python
# Loads salt scores (commander "saltiness" ratings)
# - oracle_id, salt_score, rank
EdhrecSaltProvider.get_salt_data()
```

### CardMarket (Optional)

```python
# Loads EU market identifiers
# - Set to skip_mcm=True to skip (slow API)
CardMarketProvider.get_all_products()
```

## Usage in Pipeline

The GlobalCache is accessed through PipelineContext:

```python
# In __main__.py dispatcher
cache = GlobalCache()
cache.load_all(set_codes=args.sets)

# Create context wrapping the cache
ctx = PipelineContext.from_global_cache(args)

# Access data through context properties
cards = ctx.cards_lf  # Delegates to cache.cards_lf
```

## Memory Considerations

| Data | Approx Size (Memory) | As LazyFrame |
|------|---------------------|--------------|
| cards_lf | 2-4 GB | ~100 MB pointer |
| mcm_lookup_lf | 500 MB | ~50 MB pointer |
| All providers | 5-8 GB total | ~500 MB pointers |

The `_dump_and_reload_as_lazy()` step is critical for keeping memory usage reasonable during the pipeline.

## Testing

For testing without the full GlobalCache:

```python
# PipelineContext.for_testing() bypasses GlobalCache
ctx = PipelineContext.for_testing(
    cards_lf=pl.LazyFrame({...}),
    sets_lf=pl.LazyFrame({...}),
)
```

This allows unit tests to inject specific test data without loading all providers.

## Adding a New Provider

To integrate a new external data source into the pipeline:

### 1. Create the provider module

```
mtgjson5/v2/providers/{name}/
├── __init__.py
└── provider.py     # class {Name}Provider with static fetch methods
```

Each provider should be self-contained — no dependencies on other providers. Fetch methods should return a `pl.DataFrame`.

### 2. Add LazyFrame to GlobalCache

In `cache.py`, add:
- A new `{name}_lf: pl.LazyFrame | None` attribute
- A load call in `load_all()` (typically inside the parallel thread pool)
- The attribute to `_dump_and_reload_as_lazy()` so it gets serialized to parquet and reloaded as a LazyFrame

### 3. Wire into PipelineContext

In `context.py`, add:
- A property with the `_test_data` fallback pattern:
  ```python
  @property
  def {name}_lf(self) -> pl.LazyFrame:
      if self._test_data and "{name}_lf" in self._test_data:
          return self._test_data["{name}_lf"]
      return self._cache.{name}_lf
  ```
- A parameter to `for_testing()` so tests can inject mock data

### 4. Use in the pipeline

In `core.py`, add a join step in the appropriate stage (typically Stage 4 for lookup joins). If the data joins by `oracleId`, consider adding it to `_build_oracle_data_lookup()` in the context instead.

See the [Contributor Guide](contributing.md#new-external-data-source) for the complete decision tree.
