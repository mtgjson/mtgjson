# Pipeline Core - Main Transformation

**File**: `mtgjson5/v2/pipeline/core.py`

The `build_cards()` function is the heart of the Polars pipeline. It transforms raw Scryfall data into MTGJSON format through a series of lazy transformations with strategic materialization checkpoints.

## Entry Point

```python
from mtgjson5.v2.pipeline.core import build_cards

# After loading cache and building context
build_cards(ctx)

# Output: Partitioned parquet files in CACHE_PATH/_parquet/
```

## Pipeline Stages Overview

The pipeline consists of 12 major stages:

```
1.  Load + Filter          → Filter to requested sets and languages
2.  Per-Card Transforms    → Streaming-safe transformations (face explosion, fields, types, mana, legalities)
3.  Multi-Row Joins        → Join derived lookup tables (identifiers, oracle, set/number, name, MCM)
4.  CHECKPOINT 1           → Materialize and reset lazy plan (after joins)
5.  Struct Assembly         → Build identifier/UUID structs
6.  Duel Deck + Gatherer   → Duel deck detection, Gatherer data join
7.  CHECKPOINT 2           → Materialize and reset lazy plan (before relationship ops)
8.  Relationship Ops       → Self-joins: otherFaceIds, leadershipSkills, reverseRelated, tokenIds, purchaseUrls
9.  CHECKPOINT 3           → Materialize and reset lazy plan (before final enrichment)
10. Final Enrichment       → Manual overrides, rebalanced linkage, secret lair subsets, source products
11. Signatures + Cleanup   → Signature data, drop raw Scryfall columns
12. Sink to Parquet        → Partitioned output
```

## Stage 1: Load and Filter

```python
def build_cards(ctx: PipelineContext) -> None:
    set_codes = ctx.set_codes

    # Start with raw Scryfall cards
    lf = ctx.cards_lf

    # Filter to requested sets
    lf = lf.filter(pl.col("set").is_in(set_codes))

    # Language filtering is done inline via a join pattern
    # (not a standalone function) — filters to English cards
    # or non-English cards without an English equivalent

    # Optional: filter to deck-only scryfallIds
    if ctx.deck_scryfall_ids:
        lf = lf.filter(pl.col("id").is_in(ctx.deck_scryfall_ids))
```

## Stage 2: Per-Card Transformations

These transformations can stream row-by-row without collecting:

### Face Explosion and Meld Handling

```python
lf = explode_card_faces(lf)
lf = assign_meld_sides(lf, ctx)
lf = update_meld_names(lf, ctx)
lf = detect_aftermath_layout(lf)
```

Multi-face cards (split, transform, adventure, meld) are exploded into separate rows. Meld cards get special side assignment (parts = "a", result = "b").

### Field Extraction and Enrichment

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
lf = join_face_flavor_names(lf, ctx)
```

Extracts and normalizes card fields (name, manaCost, colors, type, text, power/toughness, loyalty), adds booster types, fixes promo types, enriches card data, and handles watermarks.

### Type Line Parsing and Mana

```python
lf = parse_type_line_expr(lf)
lf = add_mana_info(lf)
lf = fix_manavalue_for_multiface(lf)
```

Parses type lines into supertypes/types/subtypes. Computes manaValue, colorIdentity, and colors.

### Card Attributes

```python
lf = add_card_attributes(lf)
lf = filter_keywords_for_face(lf)
```

Adds keywords, isReserved, isReprint, and filters keywords to face-specific ones.

### Legalities and Availability

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

## Stage 4: Multi-Row Joins

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

### Availability Fix from IDs

```python
lf = fix_availability_from_ids(lf)
```

Adds platforms to availability if their respective ID fields are present (e.g., adds "mtgo" if mtgoId exists).

## Stage 5: Checkpoint 2

```python
lf = lf.collect().lazy()
```

Resets query plan after join operations.

## Stage 6: Struct Assembly and UUIDs

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
lf = calculate_duel_deck(lf, ctx)
lf = join_gatherer_data(lf, ctx)
```

Determines duel deck sides and joins Gatherer page data.

## Stage 7: Checkpoint 2

```python
lf = lf.collect().lazy()
```

**Why checkpoint here?**
- Resets before relationship operations (self-joins, cross-row lookups)
- Prevents query plan explosion from self-join operations

## Stage 8: Relationship Operations

These operations require self-joins or cross-row lookups:

```python
lf = add_other_face_ids(lf, ctx)
lf = add_leadership_skills_expr(lf, ctx)
lf = add_reverse_related(lf)
lf = add_token_ids(lf, scryfall_uuid_lf)
lf = propagate_salt_to_tokens(lf)
lf = add_related_cards_from_context(lf, ctx)
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

## Stage 9: Checkpoint 3

```python
lf = lf.collect().lazy()
```

Resets before final enrichment.

## Stage 10: Final Enrichment

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

### Signatures and Cleanup

```python
lf = join_signatures(lf, ctx)
lf = add_signatures_combined(lf, ctx)
lf = drop_raw_scryfall_columns(lf)
```

Adds signature data for signed cards, then removes intermediate Scryfall columns not needed in output.

## Stage 12: Sink to Parquet

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

### `face_field()`

Extracts field from face data or falls back to card level:

```python
def face_field(field_name: str) -> pl.Expr:
    return pl.coalesce(
        pl.col("_face_data").struct.field(to_snake_case(field_name)),
        pl.col(field_name)
    )
```

### `sort_colors_wubrg_expr()`

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

### `calculate_cmc_expr()`

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

**File**: `mtgjson5/v2/pipeline/expressions.py`

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

### Choosing the right stage

| If your transform... | Place it in... |
|----------------------|----------------|
| Operates per-card, no joins needed | **Stage 2** (Per-Card Transforms) |
| Needs data from a lookup table (context join) | **Stage 3** (Multi-Row Joins) |
| Needs struct assembly or UUID data | **Stage 5** (Struct Assembly) |
| Needs self-joins (otherFaceIds, variations) | **Stage 8** (Relationship Operations) |
| Applies overrides or final enrichment | **Stage 10** (Final Enrichment) |

### Template

```python
def add_my_field(lf: pl.LazyFrame, ctx: PipelineContext) -> pl.LazyFrame:
    """Add myField to the pipeline."""
    return lf.with_columns(
        # Use Polars expressions, not Python UDFs
        pl.col("source_column").str.to_uppercase().alias("myField")
    )
```

Then call it from `build_cards()` in the appropriate stage:

```python
# In the relevant stage section of build_cards()
lf = add_my_field(lf, ctx)
```

### Checklist

- Use Polars-native expressions (no `map_elements` or `.apply()`)
- If adding a join, consider whether a new checkpoint is needed (>3 joins since last checkpoint)
- Add the corresponding model field to `v2/models/cards.py`
- Write a test using `make_card_lf()` and `PipelineContext.for_testing()`
- If the transform uses a new expression, add it to `v2/pipeline/expressions.py` with its own unit test
