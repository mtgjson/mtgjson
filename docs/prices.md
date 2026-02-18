# Price Engine

**Files**:
- `mtgjson5/build/price_builder.py` — Orchestrator (`PriceBuilderContext`, `PolarsPriceBuilder`)
- `mtgjson5/build/price_archive.py` — Archive management (load, save, merge, prune, partition, migrate)
- `mtgjson5/build/price_writers.py` — Output writers (JSON streaming, SQLite, SQL, PostgreSQL, CSV)
- `mtgjson5/build/price_s3.py` — S3 sync operations
- `mtgjson5/build/referral_builder.py` — Referral map generation for purchase URL rewrites

The price engine is a distinct ETL pipeline from the card builder. It fetches daily prices from five providers, stores them in a date-partitioned parquet data lake, syncs to/from S3, and streams JSON and SQL outputs — all without loading the full history into memory.

## Overview

```
┌──────────────────────────────────────────────────────┐
│  1. PriceBuilderContext         (price_builder.py)   │
│  Builds ID mappings (tcgplayerId → UUID, etc.)       │
│  from GlobalCache LazyFrames                         │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  2. Provider Fetch              (price_builder.py)   │
│  TCGPlayer, CardHoarder, Manapool, CardMarket, CK    │
│  → flat DataFrame with PRICE_SCHEMA                  │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  3. Date-Partitioned Storage    (price_archive.py)   │
│  prices/date=2024-02-07/data.parquet                 │
│  (zstd compressed, level 9)                          │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  4. S3 Sync                     (price_s3.py)        │
│  Upload today's partition, download missing ones     │
│  S3 is append-only (never pruned)                    │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  5. Prune + Load 90-Day Window  (price_archive.py)   │
│ Delete old LOCAL partitions, load recent as LazyFrame│
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│  6. Output                      (price_writers.py)   │
│  AllPrices.json (streamed), AllPricesToday.json      │
│  AllPricesToday.sqlite, .sql, .psql                  │
└──────────────────────────────────────────────────────┘
```

## Relationship to the Card Pipeline

The price engine runs **independently** from the card assembly pipeline. It is not part of `AssemblyContext` or `UnifiedOutputWriter`:

```
Card Pipeline:  GlobalCache → PipelineContext → build_cards() → AssemblyContext → OutputWriter
Price Pipeline: PriceBuilderContext → Provider Fetch → Partition Store → S3 Sync → JSON/SQL Output
```

Prices are triggered by the `--price-build` / `-PB` flag, which runs after the card pipeline completes (if both are requested).

## CLI Usage

```bash
# Standalone price build
python -m mtgjson5 --price-build

# Full build + prices
python -m mtgjson5 --build-all --price-build
```

## Price Schema

All providers produce rows in a single flat tabular schema (defined in `price_archive.py`):

```python
PRICE_SCHEMA = {
    "uuid":       pl.String,    # MTGJSON UUID
    "date":       pl.String,    # "YYYY-MM-DD"
    "source":     pl.String,    # "paper" or "mtgo"
    "provider":   pl.String,    # "tcgplayer", "cardhoarder", "manapool", "cardmarket", "cardkingdom"
    "price_type": pl.String,    # "buylist" or "retail"
    "finish":     pl.String,    # "normal", "foil", "etched"
    "price":      pl.Float64,   # Price value
    "currency":   pl.String,    # "USD" or "EUR"
}
```

One row per `(uuid, date, source, provider, price_type, finish)` combination.

## PriceBuilderContext (`price_builder.py`)

**Purpose**: Build ID mappings so providers can convert their internal IDs to MTGJSON UUIDs.

```python
ctx = PriceBuilderContext.from_cache()
```

### Mappings

| Mapping | Key → Value | Used By |
|---------|-------------|---------|
| `tcg_to_uuid` | TCGPlayer productId → set[UUID] | TCGPlayer |
| `tcg_etched_to_uuid` | TCGPlayer etched productId → set[UUID] | TCGPlayer |
| `mtgo_to_uuid` | MTGO ID → set[UUID] | CardHoarder |
| `scryfall_to_uuid` | Scryfall ID → set[UUID] | Manapool |

Mappings are built lazily from GlobalCache LazyFrames on first access. For standalone price builds (no preceding card build), `load_id_mappings()` reads from previously-written parquet files.

## PolarsPriceBuilder (`price_builder.py`)

The main orchestrator class that coordinates provider fetching and delegates to the archive, S3, and writer modules.

### Key Methods

| Method | Purpose |
|--------|---------|
| `build_today_prices_async()` | Async fetch from 5 providers, returns flat DataFrame |
| `build_today_prices()` | Sync wrapper around `build_today_prices_async()` |
| `build_prices()` | Full pipeline: migrate → S3 sync → fetch → save → upload → prune |
| `build_prices_parquet()` | Returns DataFrames instead of writing dicts |
| `to_nested_dict()` | Converts flat DataFrame to nested MTGJSON JSON format |

The `build_prices()` method orchestrates the full pipeline by calling functions from the other modules:

```python
def build_prices(self):
    migrate_legacy_archive()                    # price_archive.py
    sync_missing_partitions_from_s3(days=90)    # price_s3.py
    today_df = self.build_today_prices()        # provider fetch
    save_prices_partitioned(today_df, today)    # price_archive.py
    sync_partition_to_s3(today)                 # price_s3.py
    prune_partitions(days=90)                   # price_archive.py
    lf = load_partitioned_archive(days=90)      # price_archive.py
    stream_write_all_prices_json(lf, path, today)  # price_writers.py
```

## Providers

### TCGPlayer (`providers/tcgplayer/prices.py`)

- **Source**: `paper` | **Currency**: `USD`
- **Pricing**: Retail only (buylist API deprecated)
- **Volume**: ~250k-500k price points per day (largest provider)
- **Method**: Async streaming with checkpointing — streams Magic sets via pagination, saves progress to `.tcg_price_checkpoint.json` every 50 sets, resumes from checkpoint on restart

### CardHoarder (`providers/cardhoarder/provider.py`)

- **Source**: `mtgo` | **Currency**: `USD`
- **Pricing**: Retail (normal + foil, parallel requests)
- **Method**: Bulk TSV download from affiliate pricefile endpoint

### Manapool (`providers/manapool/provider.py`)

- **Source**: `paper` | **Currency**: `USD`
- **Pricing**: Retail (prices in cents, converted to dollars)
- **Method**: Single bulk API endpoint, maps scryfall_id → UUID

### CardMarket (`providers/cardmarket/provider.py`)

- **Source**: `paper` | **Currency**: `EUR`
- **Pricing**: Retail + buylist
- **Method**: Sequential requests via mkmsdk (rate limited to 1 request per 1.5s)

### Card Kingdom (`providers/cardkingdom/provider.py`)

- **Source**: `paper` | **Currency**: `USD`
- **Pricing**: Retail + buylist
- **Method**: Async fetch with parquet caching; facade over Client + Transformer + PriceProcessor + Storage

### Provider Output

All providers produce DataFrames in `PRICE_SCHEMA` format. They are concatenated:

```python
frames: list[pl.DataFrame] = []
# ... collect from all providers ...
return pl.concat(frames) if frames else pl.DataFrame(schema=PRICE_SCHEMA)
```

## Date-Partitioned Data Lake (`price_archive.py`)

### Structure

```
.mtgjson5_cache/prices/
├── date=2024-01-28/
│   └── data.parquet
├── date=2024-01-29/
│   └── data.parquet
├── date=2024-01-30/
│   └── data.parquet
├── ...
└── date=2024-04-28/
    └── data.parquet
```

Each partition is a single zstd-compressed parquet file (compression level 9) containing all provider prices for that date.

### Why date-partitioned?

Polars' `scan_parquet()` with hive partitioning pushes date filters down to partition pruning. Loading a 90-day window reads only ~90 small files instead of scanning years of history:

```python
# Reads ONLY the partitions within the date range — skips everything else
lf = pl.scan_parquet(
    PRICES_PARTITION_DIR / "**/*.parquet",
    hive_partitioning=True,
).filter(pl.col("date") >= cutoff_date)
```

A full price history might span 5+ years (~1,800+ partitions). Without date partitioning, every query would scan the entire dataset. With partitions, a 90-day window touches only 90 files — ~20x less I/O.

### Archive Functions

| Function | Purpose |
|----------|---------|
| `save_prices_partitioned(df, today_date)` | Write today's prices to `date={today}/data.parquet` |
| `load_partitioned_archive(days=90)` | Scan partitions within retention window as LazyFrame |
| `prune_partitions(days=90)` | Delete local partitions older than cutoff |
| `prune_prices(df, months=3)` | Filter DataFrame rows older than cutoff |
| `merge_prices(archive, today)` | Concat + dedup, keeping latest |
| `migrate_legacy_archive()` | One-time migration from single-file archive to partitioned format |
| `load_archive(path)` | Load from single parquet or JSON file |
| `save_archive(df, path)` | Save as parquet with zstd compression |
| `list_local_partitions()` | List available local date partitions |

## 90-Day Window and Pruning

### Strategy

| Location | Retention | Pruning |
|----------|-----------|---------|
| **Local** | 90 days | Old partitions deleted by `prune_partitions()` |
| **S3** | Indefinite | Append-only, never pruned |
| **JSON output** | 90 days | `load_partitioned_archive(days=90)` filters before output |

### Pruning Logic

```python
def prune_prices(df: pl.LazyFrame, months: int = 3) -> pl.LazyFrame:
    cutoff = (
        datetime.date.today()
        + dateutil.relativedelta.relativedelta(months=-months)
    ).strftime("%Y-%m-%d")
    return df.filter(pl.col("date") >= cutoff)
```

Local partitions are pruned to save disk space. S3 keeps the complete historical archive — if a full rebuild is ever needed, `sync_missing_partitions_from_s3()` can repopulate the local data lake.

## S3 Sync (`price_s3.py`)

### Path Format

```
s3://{bucket_name}/price_archive/date=2024-02-07/data.parquet
```

Configuration comes from `mtgjson.properties` under the `[Prices]` section:

```ini
[Prices]
bucket_name = my-mtgjson-bucket
```

### Sync Functions

| Function | Direction | Description |
|----------|-----------|-------------|
| `sync_partition_to_s3(date)` | Local → S3 | Upload a single date partition |
| `sync_partition_to_s3_with_retry(date)` | Local → S3 | Upload with exponential backoff (3 retries) |
| `sync_local_partitions_to_s3(days=90)` | Local → S3 | Batch upload missing partitions (16 threads) |
| `sync_missing_partitions_from_s3(days=90)` | S3 → Local | Download partitions we don't have locally |
| `list_s3_partitions()` | S3 | List available date partitions (boto3 paginator) |
| `get_price_archive_from_s3()` | S3 → Local | Download full archive |
| `upload_archive_to_s3()` | Local → S3 | Upload archive |

### Sync in `build_prices()` Flow

1. **Before fetch**: `sync_missing_partitions_from_s3()` — fill in any gaps from S3
2. **After fetch**: `sync_partition_to_s3()` — upload today's new partition (non-fatal on failure)

## Merging and Deduplication

When today's prices overlap with the archive (e.g., a retry on the same day), deduplication uses last-wins on the composite key:

```python
key_cols = ["uuid", "date", "source", "provider", "price_type", "finish"]
combined = pl.concat([archive, today])
return combined.group_by(key_cols).agg([
    pl.col("price").last(),
    pl.col("currency").last(),
])
```

## Output Generation (`price_writers.py`)

### JSON — Streamed by UUID Prefix

`AllPrices.json` can be very large (~500MB+). The writer streams it using UUID prefix chunking to limit memory:

1. Partition UUIDs by hex prefix (0-9, a-f) — 16 chunks
2. Materialize one chunk at a time
3. Sort within each chunk by uuid, source, provider, price_type, finish, date
4. Write to the output stream and release memory

**Nested JSON structure**:

```json
{
  "meta": { "date": "2024-02-07", "version": "5.x.x" },
  "data": {
    "uuid-1": {
      "paper": {
        "tcgplayer": {
          "retail": {
            "normal": { "2024-02-01": 1.50, "2024-02-02": 1.45 },
            "foil":   { "2024-02-01": 3.00 }
          },
          "currency": "USD"
        },
        "cardkingdom": {
          "retail":  { "normal": { "2024-02-01": 1.99 } },
          "buylist": { "normal": { "2024-02-01": 0.80 } },
          "currency": "USD"
        }
      },
      "mtgo": {
        "cardhoarder": {
          "retail": { "normal": { "2024-02-01": 0.02 } },
          "currency": "USD"
        }
      }
    }
  }
}
```

### AllPricesToday.json

Same nested structure, but filtered to today's date only (much smaller).

### Writer Functions

| Function | Output | Description |
|----------|--------|-------------|
| `stream_write_all_prices_json(lf, path, today_date)` | `.json` | Stream AllPrices.json by UUID prefix (16 chunks) |
| `stream_write_today_prices_json(df, path, today_date)` | `.json` | Write today's prices only |
| `write_prices_sqlite(df, path)` | `.sqlite` | Binary SQLite with indexes |
| `write_prices_sql(df, path)` | `.sql` | SQL INSERT statements (batch size: 10,000 rows) |
| `write_prices_psql(df, path)` | `.psql` | PostgreSQL COPY format |
| `write_prices_csv(df, path)` | `.csv` | CSV matching v1 cardPrices.csv format |

**SQLite Schema**:

```sql
CREATE TABLE "prices" (
    "uuid"      TEXT,
    "date"      TEXT,
    "source"    TEXT,
    "provider"  TEXT,
    "priceType" TEXT,
    "finish"    TEXT,
    "price"     REAL,
    "currency"  TEXT
);

CREATE INDEX "idx_prices_uuid"     ON "prices" ("uuid");
CREATE INDEX "idx_prices_date"     ON "prices" ("date");
CREATE INDEX "idx_prices_provider" ON "prices" ("provider");
```

## Referral Map Builder (`referral_builder.py`)

Generates `ReferralMap.json` for Nginx URL rewrites that redirect `/links/{hash}` to actual purchase URLs with referral codes.

### Entry Points

| Function | Purpose |
|----------|---------|
| `build_and_write_referral_map(ctx, parquet_dir, sealed_df, output_path)` | Main entry: build from cards + sealed, deduplicate, write |
| `build_referral_map_from_context(ctx, parquet_dir)` | Build entries from card pipeline data |
| `build_referral_map_from_sealed(sealed_df)` | Build entries from sealed products |
| `write_referral_map(referral_df, output_path)` | Write Nginx map format file |

### Providers

- **Card Kingdom**: Hashes `sha256(ck_base + url_path + uuid)[:16]` → referral URL with `?partner=mtgjson` suffix. Covers normal, foil, and etched variants from `identifiers_lf`.
- **TCGPlayer**: Hashes `sha256(tcg_product_id + uuid)[:16]` → partner affiliate URL. Reads `tcgplayerProductId` from parquet identifiers struct.
- **Cardmarket**: Hashes `sha256(mcm_id + uuid + BUFFER + mcm_meta_id)[:16]` → Scryfall cardmarket URL with "scryfall" replaced by "mtgjson". Joins raw Scryfall `purchaseUris` with parquet output.

### Output Format

Nginx map file with one entry per line:

```
/links/a1b2c3d4e5f6g7h8	https://www.cardkingdom.com/path?partner=mtgjson;
/links/b2c3d4e5f6g7h8i9	https://partner.tcgplayer.com/...;
```

### Dependencies

- `polars_hash` for SHA-256 hashing (via `.chash.sha2_256()`)
- Pipeline parquet output (for TCGPlayer/Cardmarket entries)
- `PipelineContext.identifiers_lf` (for Card Kingdom entries)
- Sealed products DataFrame (optional, for sealed product entries)

## Models

### TypedDicts (`models/submodels.py`)

```python
class PricePoints(TypedDict, total=False):
    etched: dict[str, float]     # date -> price
    foil: dict[str, float]
    normal: dict[str, float]

class PriceList(TypedDict, total=False):
    buylist: PricePoints
    currency: Required[str]      # "USD" or "EUR"
    retail: PricePoints

class PriceFormats(TypedDict, total=False):
    mtgo: dict[str, PriceList]   # provider -> PriceList
    paper: dict[str, PriceList]
```

### File Model (`models/files.py`)

```python
class AllPricesFile(RecordFileBase):
    data: dict[str, PriceFormats]   # UUID -> PriceFormats

    def get_prices(self, uuid: str) -> PriceFormats | None
```

## Constants

| Constant | Value | Location |
|----------|-------|----------|
| `PRICES_PARTITION_DIR` | `CACHE_PATH / "prices"` | `price_archive.py` |
| `PRICE_SCHEMA` | 8-column flat schema | `price_archive.py` |
| Default retention | 90 days | `price_archive.py` / `price_builder.py` |
| Parquet compression | zstd, level 9 | `price_archive.py` |
| S3 base path | `"price_archive"` | `price_s3.py` |
| S3 upload workers | 16 | `price_s3.py` |
| S3 max retries | 3 | `price_s3.py` |
| SQL batch size | 10,000 rows | `price_writers.py` |
| TCGPlayer checkpoint | every 50 sets | `providers/tcgplayer/prices.py` |
| CardMarket rate limit | 1.5s per request | `providers/cardmarket/provider.py` |
