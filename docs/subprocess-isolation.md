# Subprocess Isolation

This document covers how MTGJSON uses `multiprocessing.spawn` subprocesses to control memory usage during heavy build phases.

## Problem: jemalloc Memory Retention

Polars uses jemalloc as its allocator. jemalloc is fast but **never returns freed memory to the OS** within a process. During the JSON assembly phase, each stage allocates large DataFrames that persist in the process's RSS even after Python drops all references:

```
AllPrintings  → +1,856 MB  (cumulative: 3,975 MB)
AtomicCards   → +1,242 MB  (cumulative: 5,217 MB)
SetFiles      →   -1,033 MB  (GC reclaims Python objects, but jemalloc keeps pages)
TcgplayerSkus → +1,145 MB  (cumulative: 5,902 MB)
                              Peak RSS: 5,906 MB
```

The only way to fully reclaim jemalloc allocations is to **exit the process**.

## Solution: Subprocess-Per-Group

Each group of assembly tasks runs in a spawned child process. The child loads data from the parquet cache (`AssemblyContext.from_cache()`), builds its outputs, writes them to disk, then exits. All of the child's jemalloc allocations are reclaimed by the OS on process death.

```
Parent process (orchestrator, ~500 MB)
  │
  ├─ spawn → Group A: AllPrintings + FormatPrintings  (≤2 GB, exits)
  ├─ spawn → Group D: DeckFiles + DeckList             (≤0.6 GB, exits)
  ├─ spawn → Group F: AllIdentifiers + EnumValues + ... (≤0.3 GB, exits)
  ├─ spawn → Group B: AtomicCards + FormatAtomics       (≤1.5 GB, exits)
  ├─ spawn → Group C: SetFiles + SetList + Meta + ...   (≤1.5 GB, exits)
  └─ spawn → Group E: TcgplayerSkus                     (≤1.2 GB, exits)
```

Peak RSS drops from ~5.9 GB to ~3.5 GB (with 2 concurrent subprocesses) or ~2.5 GB (sequential).

## Architecture

### Two Subprocess Targets

| File | Purpose | Used By |
|------|---------|---------|
| `_subprocess_assembly.py` | JSON assembly tasks (AllPrintings, AtomicCards, etc.) | `JsonOutputBuilder.write_all()` |
| `_subprocess_exports.py` | Format exports (SQLite, CSV, Parquet, PostgreSQL) + prices | `__main__.py` |

Both follow the same design pattern:

1. **No top-level side effects** — `multiprocessing.spawn` re-imports the module in the child; heavy init code in `__main__.py` (logger setup, urllib3 warnings) must not re-execute.
2. **Disk-backed only** — children load all data from the parquet/JSON cache via `AssemblyContext.from_cache()`. No in-memory state is transferred from parent to child.
3. **Selective cache loading** — `from_cache(skip=...)` accepts a frozenset of field names to skip (e.g. `"decks"`, `"sealed"`, `"token_products"`, `"boosters"`). Each subprocess group only loads the data it actually needs, avoiding unnecessary memory use.
4. **Error propagation via Queue** — exceptions are serialized as strings and put on an error `Queue`. The parent checks the queue after `proc.join()`.
5. **Results via Queue** — assembly results (`{"AllPrintings": 109008, ...}`) are communicated back through a results `Queue`.

### Assembly Groups

Tasks are grouped to keep intra-group dependencies internal and minimize per-group peak memory. Each group specifies which optional data fields to skip via `_GROUP_SKIP` in `_subprocess_assembly.py`:

| Group | Tasks | Skips | Data Accessed |
|-------|-------|-------|---------------|
| **A** | AllPrintings, FormatPrintings | _(none)_ | `all_cards_df`, `decks_df`, `sealed_df`, `token_products`, `booster_configs` |
| **B** | AtomicCards, FormatAtomics | decks, sealed, token_products, boosters | `all_cards_df` only |
| **C** | SetFiles, SetList, Meta, CompiledList | _(none)_ | `all_cards_df`, `decks_df`, `sealed_df`, `token_products`, `booster_configs` |
| **D** | DeckFiles, DeckList | sealed, token_products, boosters | `decks_df` + uuid_index from parquet |
| **E** | TcgplayerSkus | decks, boosters | `sealed_df`, `token_products`, TCG SKU parquets |
| **F** | AllIdentifiers, EnumValues, Keywords, CardTypes | token_products, boosters | Parquet reads, `decks_df`/`sealed_df` (EnumValues), scryfall catalogs |

**Dependencies:** FormatPrintings reads AllPrintings.json from disk (stays in Group A). FormatAtomics reads AtomicCards.json from disk (stays in Group B). No cross-group dependencies.

### Scheduler

The scheduler in `JsonOutputBuilder._write_all_subprocess()` maintains a work queue of groups and runs up to `max_concurrent` subprocesses at a time:

```python
while group_queue or active:
    # Start new groups up to max_concurrent
    while group_queue and len(active) < max_concurrent:
        label, tasks = group_queue.pop(0)
        proc, results_q, error_q = _spawn_assembly_group(tasks, ...)
        active.append((label, proc, results_q, error_q))

    # Poll active processes, collect results from finished ones
    for proc in active:
        proc.join(timeout=1.0)
        if not proc.is_alive():
            # collect results, log errors
```

Groups are ordered to avoid overlapping two heavy groups (those that load `all_cards_df`) simultaneously: **A → D → F → B → C → E**. With `max_concurrent=2`, this produces:

```
Time 0:     [A: AllPrintings 423s ≤2GB]   | [D: Decks 263s ≤0.6GB]
Time 263:   [A: still running]             | [F: Light 66s ≤0.3GB]
Time 423:   [B: Atomics 102s ≤1.5GB]      | [C: SetFiles 321s ≤1.5GB]
Time 525:   [E: TcgSkus 50s ≤1.2GB]       | [C: still running]
Time 744:   done
```

### Export and Price Subprocesses

Format exports and price builds run in **separate subprocesses** to avoid jemalloc memory accumulation. If they shared a single process, the format export phase (~2.3GB) would leave retained jemalloc pages that the price build (~4.3GB) would stack on top of, reaching ~6GB total.

```python
# In __main__.py:
del assembly_ctx, ctx, raw_fetcher
GlobalCache().clear()  # release all frames + provider instances
gc.collect()

# Phase 1: Format exports (exits → jemalloc freed)
_run_subprocess(target=run_exports, args=(fmt_list,), label="exports")

# Phase 2: Price build (clean jemalloc heap)
_run_subprocess(target=_run_price_build, args=(parquet_dir, True, raw_prices_ready), label="prices")
```

**Export subprocess** uses `UnifiedOutputWriter.from_cache(skip={"decks", "sealed", "token_products", "boosters"})` to load only card parquets + normalized tables:

```
Export subprocess (format writes only, ~2.3 GB peak)
  │
  ├─ Parquet data writes (without prices)
  │   └─ builds normalized_tables, releases card_data
  │
  └─ Format writes (sqlite, csv, psql, sql)
      └─ uses normalized_tables
```

**Price subprocess** starts fresh with no jemalloc baggage:

```
Price subprocess (clean heap, ~4.3 GB peak)
  │
  ├─ Sync partitions from S3
  ├─ Map today's prices from raw cache (pre-fetched in background thread)
  ├─ Sink AllPrices.parquet (streaming, 86 partitions → 1 file)
  ├─ Stream AllPrices.json (per-prefix scans from consolidated parquet)
  ├─ AllPricesToday.json
  └─ SQL/CSV formats
```

Price network fetches are overlapped with the card pipeline via a background thread (see `docs/price_pipeline_architecture.md`), so by the time the price subprocess runs, all raw data is already cached on disk.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MTGJSON_NO_SUBPROCESS` | unset | Set to `1` to disable subprocess isolation and run all assembly in-process (fallback mode) |
| `MTGJSON_MAX_ASSEMBLY_PROCS` | `2` | Maximum concurrent assembly subprocesses. Set to `1` for lowest memory, higher for more parallelism |

### Performance Profiles

| max_concurrent | Peak RSS | Wall Time | Use Case |
|----------------|----------|-----------|----------|
| 1 | ~2.5 GB | ~1,240s | Memory-constrained environments |
| 2 (default) | ~3.5 GB | ~750s | Balanced (recommended) |
| 3+ | ~4.5 GB | ~700s | CPU-rich, memory-available |

## Data Flow

```
Pipeline (build_cards)
    │
    ▼
save_cache()  →  Parquet files in .mtgjson5_cache/_parquet/
    │                              │
    ▼                              │
JsonOutputBuilder.write_all()      │
    │                              │
    ├─ release_card_data()         │   (parent drops cached properties)
    │                              │
    ├─ spawn Group A ──────────────┤── from_cache()                     → build AllPrintings → exit
    ├─ spawn Group D ──────────────┤── from_cache(skip={sealed,tokens}) → build DeckFiles → exit
    ├─ spawn Group F ──────────────┤── from_cache(skip={tokens,boost})  → build AllIdentifiers → exit
    │  (wait for A)                │
    ├─ spawn Group B ──────────────┤── from_cache(skip={decks,sealed,tokens,boost}) → build AtomicCards → exit
    ├─ spawn Group C ──────────────┤── from_cache()                     → build SetFiles → exit
    │  (wait for B)                │
    ├─ spawn Group E ──────────────┤── from_cache(skip={decks,boost})   → build TcgplayerSkus → exit
    │                              │
    ▼                              │
Aggregate results from queues      │
    │                              │
    ▼                              │
del ctx, GlobalCache().clear()     │   (parent releases all frames + provider instances)
    │                              │
    ▼                              │
spawn exports ─────────────────────┤── from_cache(skip={decks,sealed,tokens,boost})
    │                              │   ├─ Parquet data → release card_data
    │                              │   └─ SQLite/CSV/PostgreSQL/MySQL (uses normalized_tables)
    │                              │   (exits → jemalloc freed)
    ▼                              │
spawn prices ──────────────────────┤── clean jemalloc heap
                                   │   ├─ Sync S3 partitions
                                   │   ├─ Map today's prices from raw cache
                                   │   ├─ Sink AllPrices.parquet (streaming)
                                   │   ├─ Stream AllPrices.json (per-prefix from consolidated parquet)
                                   │   └─ AllPricesToday.json + SQL/CSV formats
```

## Adding New Assembly Tasks

To add a new output to the subprocess system:

1. **Add the builder method** to `JsonOutputBuilder` in `build/formats/json.py`
2. **Add the task dispatch** in `_subprocess_assembly.py`:
   ```python
   elif task == "MyNewOutput":
       result = builder.write_my_new_output(out / "MyNewOutput.json")
       results["MyNewOutput"] = len(result.data)
   ```
3. **Add the task to a group** in `_ASSEMBLY_GROUPS` in `json.py`:
   - If it loads `all_cards_df` (heavy): create a new group or add to an existing heavy group
   - If it's light: add to Group F
4. **Update `_GROUP_SKIP`** in `_subprocess_assembly.py` if the new task changes what data a group needs (e.g. if it accesses `decks_df`, remove `"decks"` from that group's skip set)
5. **Update the `should_build` filter** if the task name doesn't match a standard output name

## Debugging

- **Subprocess errors** are logged with full tracebacks: `Assembly subprocess X error: ...`
- **Disable subprocesses** for easier debugging: `MTGJSON_NO_SUBPROCESS=1 python -m mtgjson5 --build-all`
- **Single-process profiling**: `MTGJSON_NO_SUBPROCESS=1 python -m mtgjson5 --build-all --profile` gives per-stage RSS checkpoints (the profiler in subprocess mode only shows a single `assembly/subprocess_complete` checkpoint)
- **Check spawn context**: On Windows, `spawn` is the only option. On Linux/macOS, `spawn` is explicitly selected to avoid fork-related issues with Polars/jemalloc
