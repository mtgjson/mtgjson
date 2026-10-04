# Shared sealed recipe compiler

Both mtg-sealed-content and MTGJSON use `mtg-sealed-choices`, owned by the sealed
repository under `compiler/`. Combination selection, replacement, weight totals
and independent choice groups are implemented there once. The package also
owns recipe parsing/merging/serialization, resolution traversal, direct deck
links, card/finish identity, deck-board enumeration and reverse-index output.
Both adapters use its recursive variable-membership walker, fixing MTGJSON
previously omitting cards inside nested choices from card-to-product links.

Card/deck lookup,
UUID assignment, language selection, diagnostics and final model validation
remain consumer-specific.

MTGJSON pins version 0.3.0 through an immutable upstream source archive in
`pyproject.toml`; `uv.lock` records its SHA-256. The archive avoids checking out
Git history or downloading Git LFS data. The sealed repository installs its
local package through requirements.txt. No package-index publication is needed.

## Updating the dependency

1. Change the engine and its compact contract tests in mtg-sealed-content.
2. Bump its version for behavior/API changes and merge the sealed change.
3. Update MTGJSON's source-commit pin and run `uv lock`.
4. Run the sealed tests and the integration check below against that checkout.

The existing CI suites cover the shared package and both adapters. There is no
separate daily parity workflow: with a pinned shared implementation, an upstream
change is an explicit dependency update rather than a silent copied-code drift.

## Adapter parity

For a dependency update or adapter change, compare live product recipes without
committing another copy of the catalog:

```sh
SEALED_SOURCE_PATH=/path/to/mtg-sealed-content \
  uv run pytest tests/mtgjson5/test_sealed_choice_parity.py -v
```

This checks the installed engine through each adapter. Use the source checkout
matching the dependency pin. It covers variable-product recipes plus generated
nested/weighted cases, invalid weights and input immutability. An empty scan
fails. Card UUIDs are placeholders and language is omitted, because resolution
is outside the shared contract. Final JSON serialization is covered separately
by `test_sealed_nested_variables.py`. The optional source comparison skips when
SEALED_SOURCE_PATH is absent; the local serialization tests always run.

## Adapter boundaries

The shared Product builds adapter-supplied leaf classes and preserves the adapter
subclass inside every choice. Hooks retain status.txt versus logging, MTGJSON's
language-aware UUID lookup and omission of unresolved cards from published JSON.
YAML filesystem handling and the sealed writer's orphan/placeholder preservation
remain outside the package. Booster/deck/sealed traversal is shared through CatalogWalker. The sealed
adapter retains its legacy etched fallback hook, while the pipeline adapter
retains its language and logging hooks. The extraction preserves those
differences instead of changing finish policy.

Both AllPrintings readers use the same streaming UUID-index parser. File/network
I/O stays local, and MTGJSON's Polars index builder stays in the pipeline. The
shared package does not import ijson, requests or Polars. Catalog traversal skips
scalar metadata such as card_count; this also avoids the sealed mapper trying
to iterate a product's card count.
