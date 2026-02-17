# Pipeline Core - Main Transformation

**Orchestrator**: `mtgjson5/pipeline/core.py`
**Stage modules**: `mtgjson5/pipeline/stages/`

The `build_cards()` function is the heart of the Polars pipeline. It transforms raw Scryfall data into MTGJSON format through a series of lazy transformations with strategic materialization checkpoints. The orchestrator in `core.py` is a thin coordinator — all transform logic lives in nine stage modules under `pipeline/stages/`.

## Entry Point

```python
from mtgjson5.pipeline.core import build_cards

# After loading cache and building context
build_cards(ctx)

# Output: Partitioned parquet files in CACHE_PATH/_parquet/
```

## Pipeline Stages Overview

The pipeline consists of 12 major stages, each backed by one or more stage modules:

```
1.  Load + Filter          → core.py (inline filtering logic)
2.  Per-Card Transforms    → stages/explode.py, stages/basic_fields.py
3.  Legalities + Avail     → stages/legalities.py
4.  Multi-Row Joins        → stages/identifiers.py
5.  CHECKPOINT 1           → core.py (materialize and reset lazy plan)
6.  Struct Assembly + UUIDs → stages/identifiers.py
7.  Duel Deck + Gatherer   → stages/derived.py, stages/identifiers.py
8.  CHECKPOINT 2           → core.py (materialize before relationship ops)
9.  Relationship Ops       → stages/relationships.py, stages/signatures.py, stages/derived.py
10. CHECKPOINT 3           → core.py (materialize before final enrichment)
11. Final Enrichment       → stages/derived.py
12. Signatures + Cleanup   → stages/signatures.py, stages/output.py
13. Sink to Parquet        → stages/output.py
```

## Stage Module Reference

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| `stages/explode.py` | Face explosion, meld handling, layout detection | `explode_card_faces`, `assign_meld_sides`, `update_meld_names`, `detect_aftermath_layout` |
| `stages/basic_fields.py` | Field mapping, type parsing, mana, attributes | `add_basic_fields`, `parse_type_line_expr`, `add_mana_info`, `add_card_attributes`, `add_booster_types`, `fix_promo_types` |
| `stages/identifiers.py` | External data joins, struct assembly, UUIDs | `join_identifiers`, `join_oracle_data`, `join_set_number_data`, `add_identifiers_struct`, `add_uuid_from_cache` |
| `stages/legalities.py` | Format legalities, platform availability | `add_legalities_struct`, `add_availability_struct`, `remap_availability_values`, `fix_availability_from_ids` |
| `stages/relationships.py` | Cross-card links, variations, tokens | `add_other_face_ids`, `add_leadership_skills_expr`, `add_reverse_related`, `add_token_ids`, `propagate_salt_to_tokens` |
| `stages/derived.py` | Boolean flags, purchase URLs, enrichment, overrides | `add_is_funny`, `add_is_timeshifted`, `add_purchase_urls_struct`, `apply_manual_overrides`, `add_rebalanced_linkage` |
| `stages/signatures.py` | Signature data, related cards from context | `join_signatures`, `add_signatures_combined`, `add_related_cards_from_context` |
| `stages/metadata.py` | Set metadata, decks, sealed products (standalone builders) | `build_set_metadata_df`, `build_expanded_decks_df`, `build_sealed_products_lf` |
| `stages/output.py` | Column cleanup, renaming, parquet sink | `drop_raw_scryfall_columns`, `sink_cards`, `prepare_cards_for_json`, `rename_all_the_things` |

## Stage 1: Load and Filter

```python
def build_cards(ctx: PipelineContext) -> PipelineContext:
    set_codes = ctx.sets_to_build

    # Start with raw Scryfall cards
    lf = ctx.cards_lf

    # Filter to requested sets
    lf = lf.filter(pl.col("set").is_in(set_codes))

    # Language filtering: keep English cards, or non-English cards
    # without an English equivalent (inline join pattern)

    # Optional: filter to deck-only scryfallIds
    if ctx.scryfall_id_filter:
        lf = lf.filter(pl.col("id").is_in(ctx.scryfall_id_filter))
```

## Stage 2: Per-Card Transformations

These transformations can stream row-by-row without collecting. The orchestrator chains them using `.pipe()`:

### Face Explosion and Meld Handling (`stages/explode.py`)

```python
lf = explode_card_faces(lf)
lf = assign_meld_sides(lf, ctx)
lf = update_meld_names(lf, ctx)
lf = detect_aftermath_layout(lf)
```

Multi-face cards (split, transform, adventure, meld) are exploded into separate rows. Meld cards get special side assignment (parts = "a", result = "b").

### Field Extraction and Enrichment (`stages/basic_fields.py`)

```python
lf = add_basic_fields(lf)
lf = add_booster_types(lf)
lf = fix_promo_types(lf)
lf = apply_card_enrichment(lf, ctx)
lf = fix_power_toughness_for_multiface(lf)
lf = propagate_watermark_to_faces(lf)
lf = apply_watermark_overrides(lf, ctx)
lf = format_planeswalker_text(lf)
lf = add_original_release_date(lf)
lf = join_face_flavor_names(lf, ctx)       # stages/identifiers.py
```

Extracts and normalizes card fields (name, manaCost, colors, type, text, power/toughness, loyalty), adds booster types, fixes promo types, enriches card data, and handles watermarks.

### Type Line Parsing and Mana (`stages/basic_fields.py`)

```python
lf = parse_type_line_expr(lf)
lf = add_mana_info(lf)
lf = fix_manavalue_for_multiface(lf)
```

Parses type lines into supertypes/types/subtypes. Computes manaValue, colorIdentity, and colors.

### Card Attributes (`stages/basic_fields.py`)

```python
lf = add_card_attributes(lf)
lf = filter_keywords_for_face(lf)
```

Adds keywords, isReserved, isReprint, and filters keywords to face-specific ones.

### Legalities and Availability (`stages/legalities.py`)

```python
lf = add_legalities_struct(lf, ctx)
lf = add_availability_struct(lf, ctx)
lf = remap_availability_values(lf)
```

Structures format legality and availability data, and remaps availability values to MTGJSON format.

## Stage 3: Checkpoint 1

```python
lf = lf.collect().lazy()
```

**Why checkpoint here?**
- Per-card transforms create a large lazy query plan
- Collecting resets the plan before joins
- Prevents query optimizer explosion

## Stage 4: Multi-Row Joins (`stages/identifiers.py`)

Join operations that bring in data from derived lookup tables:

### Identifiers Join

```python
lf = join_identifiers(lf, ctx)
```

Joins `ctx.identifiers_lf` on `(scryfallId, side)`:
- `cachedUuid` - Pre-computed UUID
- `cardKingdomId` - Card Kingdom identifier
- `orientation` - Image orientation

### Oracle Data Join

```python
lf = join_oracle_data(lf, ctx)
```

Joins `ctx.oracle_data_lf` on `oracleId`:
- `edhrecSaltiness` - Salt score
- `edhrecRank` - EDH popularity rank
- `rulings` - Card rulings array
- `printings` - Set codes where card appears

### Set/Number Data Join

```python
lf = join_set_number_data(lf, ctx)
lf = fix_foreigndata_for_faces(lf)
```

Joins `ctx.set_number_lf` on `(setCode, number)`:
- `foreignData` - Non-English card data
- `duelDeck` - Duel deck side indicator

### Name Data Join

```python
lf = join_name_data(lf, ctx)
```

Joins `ctx.name_lf` on `name`:
- `cardParts` - Meld card parts
- `leadershipSkills` - Commander attributes

### CardMarket Join

```python
lf = join_cardmarket_ids(lf, ctx)
```

Adds CardMarket identifiers if available.

### Availability Fix from IDs (`stages/legalities.py`)

```python
lf = fix_availability_from_ids(lf)
```

Adds platforms to availability if their respective ID fields are present (e.g., adds "mtgo" if mtgoId exists).

## Stage 5: Checkpoint 2

```python
lf = lf.collect().lazy()
```

Resets query plan after join operations.

## Stage 6: Struct Assembly and UUIDs (`stages/identifiers.py`)

### Identifiers Struct

```python
lf = add_identifiers_struct(lf)
```

Builds the `identifiers` struct with all external IDs:
```python
{
    "scryfallId": str,
    "scryfallOracleId": str,
    "cardKingdomId": str | None,
    "cardKingdomFoilId": str | None,
    "tcgplayerId": int | None,
    "tcgplayerEtchedId": int | None,
    "cardMarketId": int | None,
    "mtgoId": int | None,
    "mtgoFoilId": int | None,
    "multiverseId": int | None,
    # ... more identifiers
}
```

### UUID Assignment

```python
lf = add_uuid_from_cache(lf, ctx)
lf = add_identifiers_v4_uuid(lf)
```

UUID assignment priority:
1. Use cached UUID if available
2. Generate new UUID v5 from card data
3. Add v4 UUID for identifiers struct

## Stage 6b: Duel Deck and Gatherer

```python
lf = calculate_duel_deck(lf)    # stages/derived.py
lf = join_gatherer_data(lf, ctx) # stages/identifiers.py
```

Determines duel deck sides and joins Gatherer page data.

## Stage 7: Checkpoint 3

```python
lf = lf.collect().lazy()
```

**Why checkpoint here?**
- Resets before relationship operations (self-joins, cross-row lookups)
- Prevents query plan explosion from self-join operations

## Stage 8: Relationship Operations

These operations require self-joins or cross-row lookups:

```python
# stages/relationships.py
lf = add_other_face_ids(lf, ctx)
lf = add_leadership_skills_expr(lf, ctx)
lf = add_reverse_related(lf)
lf = add_token_ids(lf, scryfall_uuid_lf)
lf = propagate_salt_to_tokens(lf)

# stages/signatures.py
lf = add_related_cards_from_context(lf, ctx)

# stages/derived.py
lf = add_alternative_deck_limit(lf, ctx)
lf = add_is_funny(lf, ctx)
lf = add_is_timeshifted(lf)
lf = add_purchase_urls_struct(lf)
```

- `add_other_face_ids` — links double-faced card UUIDs
- `add_leadership_skills_expr` — adds commander legality attributes
- `add_reverse_related` — reverse linkage for tokens
- `add_token_ids` — assigns token UUIDs
- `propagate_salt_to_tokens` — copies salt scores to tokens
- `add_related_cards_from_context` — combo/synergy data from Spellbook
- `add_alternative_deck_limit` — Relentless Rats, etc.
- `add_is_funny` — silver-bordered/acorn detection
- `add_is_timeshifted` — timeshifted card detection
- `add_purchase_urls_struct` — builds purchase URL struct

## Stage 9: Checkpoint 4

```python
lf = lf.collect().lazy()
```

Resets before final enrichment.

## Stage 10: Final Enrichment (`stages/derived.py`)

```python
lf = apply_manual_overrides(lf, ctx)
lf = add_rebalanced_linkage(lf, ctx)
lf = add_secret_lair_subsets(lf, ctx)
lf = add_source_products(lf, ctx)
```

- Applies fixes from `resources/manual_overrides.json`
- Links rebalanced cards (Pioneer Masters variants) to originals
- Adds Secret Lair subset information
- Links cards to sealed products they appear in

### Signatures and Cleanup (`stages/signatures.py`, `stages/output.py`)

```python
lf = join_signatures(lf, ctx)           # stages/signatures.py
lf = add_signatures_combined(lf, ctx)   # stages/signatures.py
lf = drop_raw_scryfall_columns(lf)      # stages/output.py
```

Adds signature data for signed cards, then removes intermediate Scryfall columns not needed in output.

## Stage 12: Sink to Parquet (`stages/output.py`)

```python
sink_cards(ctx)
```

Final output writes partitioned parquet files:

```
CACHE_PATH/
├── _parquet/
│   ├── setCode=MH3/
│   │   └── 0.parquet
│   ├── setCode=BLB/
│   │   └── 0.parquet
│   └── ...
└── _parquet_tokens/
    ├── setCode=TMKM/
    │   └── 0.parquet
    └── ...
```

### Sink Process

1. Filter to default language per card
2. Link foil/nonfoil variations
3. Add variations (related printings)
4. Split cards vs tokens
5. Rename columns for CardSet/CardToken models
6. Partition by setCode
7. Write parquet files

## Key Helper Functions

### `face_field()` (`stages/basic_fields.py`)

Extracts field from face data or falls back to card level:

```python
def face_field(field_name: str) -> pl.Expr:
    return pl.coalesce(
        pl.col("_face_data").struct.field(to_snake_case(field_name)),
        pl.col(field_name)
    )
```

### `sort_colors_wubrg_expr()` (`pipeline/expressions.py`)

Sorts color arrays in WUBRG order:

```python
def sort_colors_wubrg_expr(col: str = "colors") -> pl.Expr:
    # W=0, U=1, B=2, R=3, G=4
    return pl.col(col).list.eval(
        pl.element().replace_strict(COLOR_ORDER_MAP)
    ).list.sort().list.eval(
        pl.element().replace_strict(REVERSE_COLOR_MAP)
    )
```

### `calculate_cmc_expr()` (`pipeline/expressions.py`)

Pure Polars CMC calculation without Python UDFs:

```python
def calculate_cmc_expr(col: str = "manaCost") -> pl.Expr:
    """
    {2}{W}{U} -> 4.0
    {X}{R}{R} -> 2.0 (X counts as 0)
    {2/W}{2/W} -> 4.0 (hybrid takes higher)
    """
```

## Expressions Module

**File**: `mtgjson5/pipeline/expressions.py`

Pure Polars expressions for performance:

```python
# UUID generation
uuid5_expr("scryfallId")

# ASCII normalization
ascii_name_expr("name")  # Æther -> Aether

# Finish ordering
order_finishes_expr("finishes")  # nonfoil < foil < etched

# Color extraction
extract_colors_from_mana_expr("manaCost")

# WUBRG sorting
sort_colors_wubrg_expr("colors")
```

## Orchestrator Pattern

The `core.py` orchestrator uses Polars `.pipe()` chains with `functools.partial` for context injection:

```python
# Actual orchestrator pattern in core.py
lf = (
    lf.pipe(explode_card_faces)
    .pipe(partial(assign_meld_sides, ctx=ctx))
    .pipe(partial(update_meld_names, ctx=ctx))
    .pipe(detect_aftermath_layout)
    .pipe(add_basic_fields)
    # ... more stages
)
```

Functions that need the `PipelineContext` are wrapped with `partial(fn, ctx=ctx)`. Functions that operate on the LazyFrame alone are passed directly.

## Memory and Performance

### Why Checkpoints?

Without checkpoints, the lazy query plan grows exponentially:
- Each join adds to the plan
- Self-joins compound complexity
- Query optimizer struggles with huge plans

Checkpoints:
1. Materialize current state
2. Write to memory as DataFrame
3. Convert back to LazyFrame
4. Fresh query plan for next stage

### Parallel vs Sequential

Most operations are inherently parallel (Polars handles this).

Sequential operations:
- Self-joins (must complete before next step)
- UUID assignment (depends on cache lookup)
- Checkpoint writes

### Memory Usage

| Stage | Approx Memory |
|-------|---------------|
| Load cards_lf | Lazy (~100MB) |
| After Checkpoint 1 | ~2-4GB |
| After Joins | ~4-6GB |
| Final parquet | ~1GB on disk |

## Error Handling

The pipeline uses Polars' null handling:
- Missing fields → null values
- Failed joins → null in joined columns
- Type mismatches → caught at collect time

Critical errors surface at checkpoints when `.collect()` executes the plan.

## Debugging

### Print Schema at Any Point

```python
# Add temporarily for debugging
print(lf.collect_schema())
```

### Inspect Intermediate State

```python
# Collect and inspect
df = lf.collect()
print(df.head())
print(df.select("name", "problematic_column"))
```

### Row Count Tracking

```python
# Track row counts through pipeline
print(f"After explode: {lf.select(pl.len()).collect().item()}")
```

## Adding a New Transformation

### Choosing the right stage module

| If your transform... | Place it in... |
|----------------------|----------------|
| Operates per-card, no joins needed | `stages/basic_fields.py` |
| Needs face explosion or layout changes | `stages/explode.py` |
| Needs data from a lookup table (context join) | `stages/identifiers.py` |
| Affects legalities or availability | `stages/legalities.py` |
| Needs self-joins (otherFaceIds, variations) | `stages/relationships.py` |
| Adds boolean flags, derived attributes, or purchase data | `stages/derived.py` |
| Involves signatures or related card context | `stages/signatures.py` |
| Is a standalone builder (decks, sealed, set metadata) | `stages/metadata.py` |
| Relates to output cleanup, renaming, or sinking | `stages/output.py` |

### Template

```python
# In the appropriate stages/ module:

def add_my_field(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """Add myField to the pipeline."""
    return lf.with_columns(
        # Use Polars expressions, not Python UDFs
        pl.col("source_column").str.to_uppercase().alias("myField")
    )
```

Then wire it into the orchestrator in `core.py`:

```python
# In core.py, add to the appropriate pipe chain:
lf = lf.pipe(partial(add_my_field, ctx=ctx))
```

### Checklist

- Use Polars-native expressions (no `map_elements` or `.apply()`)
- If adding a join, consider whether a new checkpoint is needed (>3 joins since last checkpoint)
- Add the corresponding model field to `models/cards.py`
- Write a test using `make_card_lf()` and `PipelineContext.for_testing()`
- If the transform uses a new expression, add it to `pipeline/expressions.py` with its own unit test
