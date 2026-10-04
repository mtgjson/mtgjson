# Sealed choice compilation contract

MTGJSON compiles mtg-sealed-content recipes inline. The sealed repository also
compiles them for validation and status generation. Until their choice engine
is shared, both implementations must produce identical choice structures.

`Sealed choice parity` checks MTGJSON against the current sealed `main` on pull
requests, pushes and daily runs. Both commit hashes are logged. It compares the
live variable-product recipes and small generated cases covering nested choices,
replacement, counts, weights, rejection of invalid weights and input immutability.
An empty product scan fails, so a source-layout change cannot silently disable it.
No copied catalog or golden output is committed.

Run against an isolated source checkout:

```sh
SEALED_SOURCE_PATH=/path/to/mtg-sealed-content \
  uv run pytest tests/mtgjson5/test_sealed_choice_parity.py -v
```

An ordinary test run skips this integration check when that environment variable
is absent. CI supplies it explicitly. No AllPrintings file or credentials are
needed. Card UUIDs are placeholders and language is omitted: UUID resolution,
missing-card handling and language selection belong to each consumer, not to
this choice contract. Final JSON serialization remains covered separately by
`test_sealed_nested_variables.py`.

## Shared-package boundary

The next extraction should own only combination selection, weight calculation,
independent group merging and the recursive choice structure. It must not own
card/deck lookup, UUID assignment, language selection, logging or output storage.
The package should live in mtg-sealed-content, publish versioned releases and be
pinned by MTGJSON. Retain the contract tests as acceptance tests when switching
both callers to that package. This change adds drift detection; it does not yet
remove either compiler or introduce an unpublished dependency.
