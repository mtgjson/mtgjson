# Pipeline-Driven Documentation & Types

**Directory**: `mtgjson5/v2/models/`

The pipeline generates TypeScript type definitions and VitePress documentation pages directly from Pydantic model metadata. This makes the pipeline the single source of truth — types AND documentation live in the models, eliminating dual-maintenance between the pipeline and the [mtgjson-website](https://github.com/mtgjson/mtgjson-website).

## Quick Start

```bash
# Generate TypeScript types (combined + per-model files)
python -m mtgjson5 --generate-types

# Generate TypeScript types + documentation pages
python -m mtgjson5 --generate-types --generate-docs

# Custom output path for types
python -m mtgjson5 --generate-types /path/to/AllMTGJSONTypes.ts
```

## Output Structure

```
mtgjson_build_5.3.0/
├── AllMTGJSONTypes.ts              # Combined TypeScript definitions
├── types/
│   ├── CardAtomic.ts               # Individual per-model type files
│   ├── CardSet.ts
│   ├── DeckList.ts
│   ├── Identifiers.ts
│   ├── SealedProduct.ts
│   └── ...                         # ~70 files, one per export type
└── data-models/
    ├── deck-list/index.md           # Generated VitePress pages
    ├── deck/index.md
    ├── deck-set/index.md
    └── sealed-product/index.md
```

The `types/` and `data-models/` directories mirror the website's expected layout exactly:

| Generated path | Website path |
|---|---|
| `types/DeckList.ts` | `docs/public/types/DeckList.ts` |
| `data-models/deck-list/index.md` | `docs/data-models/deck-list/index.md` |

## TypeScript Generation

### Combined file

`write_typescript_interfaces()` in `v2/models/__init__.py` generates `AllMTGJSONTypes.ts` with all TypedDict and Pydantic models grouped by category (sub-models, cards, sets, files).

### Per-model splitting

After generating the combined file, the function splits it on `export type` boundaries and writes individual `types/{TypeName}.ts` files. The website's VitePress pages use `<<< @/public/types/DeckList.ts{TypeScript}` includes to embed these directly.

### Output contract

`TypeScriptGenerator._OUTPUT_CONTRACT` overrides optionality for fields where the pipeline guarantees a value even though the Pydantic model allows `None`:

```python
_OUTPUT_CONTRACT = {
    "CardAtomic": {"purchaseUrls": "required"},
    "DeckList": {"releaseDate": "required"},        # always present, value may be null
    "DeckSet": {"sealedProductUuids": "nullable"},   # always present, value may be null
}
```

- `"required"` — field is never absent (removes `?` from TS)
- `"nullable"` — field is always present but value can be `null` (adds `| null` to TS type)

## Documentation Generation

### Model-level metadata

Models opt into documentation generation by declaring `ClassVar` attributes:

```python
class DeckList(PolarsMixin, BaseModel):
    __doc_title__: ClassVar[str] = "Deck List"
    __doc_desc__: ClassVar[str] = (
        "The Deck List Data Model describes the meta data properties "
        "of an individual [Deck](/data-models/deck/)."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent file:** [DeckList](/downloads/all-files/#decklist)\n"
        "- **Parent property:** `data`"
    )
    __doc_enum__: ClassVar[str] = "deck"        # frontmatter enum key
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, deck list"
```

Optional class vars:

| Attribute | Purpose |
|---|---|
| `__doc_title__` | Page title and `<h1>` (required to enable generation) |
| `__doc_desc__` | Description; markdown links are stripped in frontmatter meta tags |
| `__doc_parent__` | Parent file/model reference block |
| `__doc_enum__` | Frontmatter `enum:` key for website components |
| `__doc_keywords__` | SEO meta keywords |
| `__doc_slug__` | Override auto-slug (e.g. `"card/card-atomic"` for nested paths) |
| `__doc_extra__` | Extra markdown content inserted before the TypeScript section (tips, notes) |

### Field-level metadata

Field documentation is declared via Pydantic `Field()` with `description` and `json_schema_extra`:

```python
code: str = Field(
    description="The printing deck code for the deck.",
    json_schema_extra={"introduced": "v4.3.0"},
)

commander: list[CardDeck] | None = Field(
    default=None,
    description="The card that is the Commander in this deck.",
    json_schema_extra={
        "introduced": "v5.1.0",
        "optional": True,          # shows <DocBadge type="warning" text="optional" />
    },
)

type: str = Field(
    description="The type of the deck.",
    json_schema_extra={
        "introduced": "v5.1.0",
        "enum_key": "type",         # shows <ExampleField type='type'/>
    },
)
```

#### `json_schema_extra` keys

| Key | Type | Effect |
|---|---|---|
| `introduced` | `str` | **Required for field to appear in docs.** Version tag (e.g. `"v4.3.0"`) |
| `optional` | `bool` | Adds `<DocBadge type="warning" text="optional" />` badge; strips `\| null` from displayed type |
| `deprecated` | `bool` | Adds `<DocBadge type="danger" text="deprecated" />` badge |
| `deprecated_msg` | `str` | Italic deprecation message below description |
| `enum_key` | `str` | Renders `<ExampleField type='...'/>` component |
| `example` | `str` | Renders `**Example:** \`value\`` line |

Fields **without** `introduced` in their `json_schema_extra` are silently omitted from the generated page. This lets internal/undocumented fields coexist in the model without appearing in the docs.

### Optional vs nullable convention

The generator distinguishes between two concepts:

- **Optional badge** (`"optional": True`) — field may be absent from the JSON output. Type displays without `| null`.
- **Nullable type** (no `"optional"` flag) — field is always present but value can be `null`. Type displays as `string | null`.

This matches the website's existing convention:

```markdown
<!-- optional: field may be absent -->
> ### cardCount <DocBadge type="warning" text="optional" />
> - **Type:** `number`

<!-- nullable: field always present, value can be null -->
> ### subtype
> - **Type:** `string | null`
```

### MarkdownDocGenerator

`MarkdownDocGenerator` in `v2/models/utils.py` handles page generation:

```python
class MarkdownDocGenerator:
    @classmethod
    def from_model(cls, model: type[BaseModel]) -> str:
        """Generate a complete VitePress markdown page."""

    @classmethod
    def slug_from_title(cls, title: str) -> str:
        """'Deck List' -> 'deck-list'"""
```

The generated page includes:
1. YAML frontmatter (title, enum, og:title, description, keywords)
2. Title heading and description (with markdown links)
3. Parent file/model reference
4. TypeScript model include (`<<< @/public/types/{Name}.ts`)
5. Model properties in blockquote format, sorted alphabetically by output name

## Website Integration

The generated `types/` and `data-models/` directories match the website's layout exactly. Options for seamless integration are:

### CDN fetch at build time

Since the build output is uploaded to S3 via `--aws-s3-upload-bucket`, the website can fetch generated files during its build:

```js
// scripts/fetch-generated.mjs (website prebuild)
const CDN = "https://mtgjson.com/api/v5";
// Fetch types/*.ts → docs/public/types/
// Fetch data-models/*/index.md → docs/data-models/*/index.md
```

### CI artifact

The MTGJSON CI runs `--generate-types --generate-docs`, then publishes the output as a build artifact. The website CI pulls the artifact before running `vitepress build`.

### Incremental adoption

Only models with `__doc_title__` metadata generate doc pages. Non-enriched models keep the website's existing hand-maintained pages. As models are enriched, they progressively replace manual pages.


## Adding Documentation to a New Model

1. Add class-level metadata (`__doc_title__`, `__doc_desc__`, etc.)
2. Add `description` and `json_schema_extra={"introduced": "vX.Y.Z"}` to each field that should appear in docs
3. Set `"optional": True` for fields that may be absent from the JSON output
4. Run `python -m mtgjson5 --generate-types --generate-docs`
5. Compare the generated `data-models/{slug}/index.md` against the website's existing page
