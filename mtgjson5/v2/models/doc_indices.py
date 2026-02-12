"""
Index page definitions for VitePress data-models documentation.

Each entry defines a parent index page that groups related model pages.
These are not tied to any specific Pydantic model or TypedDict — they are
static navigation/overview pages required by MTGJSON's VitePress site structure.
"""

from __future__ import annotations

from typing import TypedDict


class IndexPageDef(TypedDict):
    """Definition for a VitePress parent index page."""

    slug: str
    title: str
    description: str
    keywords: str
    body: str


INDEX_PAGES: list[IndexPageDef] = [
    # ── Top-level data-models index ──────────────────────────────────
    {
        "slug": "",
        "title": "Data Models",
        "description": (
            "Data Models in MTGJSON describe any JSON object or nested JSON"
            " object that provides a flat response. As such, they will only"
            " have one level of nested keys. Any additional nested keys that"
            " return another object that is also a flat response are they"
            " themselves, a Data Model, and will have its own documentation."
        ),
        "keywords": "mtg, magic the gathering, mtgjson, json, data models",
        "body": (
            "Data Models in MTGJSON describe any JSON structure within a file"
            " or other Data Model. These are the response structures of the"
            " JSON payload.\n"
            "\n"
            "## Attributes\n"
            "\n"
            "Certain Data Models and Data Model properties may have different"
            " attributes to denote alternate types of data returned. Use the"
            " below to guide you in understanding what data flows through your"
            " application and when to use it.\n"
            "\n"
            "### Optional Properties\n"
            "\n"
            '<DocBadge inline="true" type="warning" text="optional" />'
            " Property may not return in the Data Model.\n"
            "\n"
            "### Deprecated Properties\n"
            "\n"
            '<DocBadge inline="true" type="danger" text="deprecated" />'
            " Property is deprecated in the Data Model.\n"
        ),
    },
    # ── Card group index ─────────────────────────────────────────────
    {
        "slug": "card",
        "title": "Card",
        "description": (
            "A Card is a data structure with variations of Data Models that is"
            " found within files that reference cards, and is not a Data Model"
            " itself."
        ),
        "keywords": "mtg, magic the gathering, mtgjson, json, card",
        "body": (
            "A Card is a data structure with variations of Data Models that is"
            " found within files that reference cards, and is not a Data Model"
            " itself.\n"
            "\n"
            "- **Parent model:** [Set](/data-models/set/),"
            " [Deck](/data-models/deck/),"
            " [Deck (Set)](/data-models/deck-set/)\n"
            "- **Parent property:** `cards`, `tokens`, `mainBoard`,"
            " `sideBoard`, `commander`\n"
            "\n"
            "## Card Data Models\n"
            "\n"
            "### Overview\n"
            "\n"
            "While there is no high-level structure of a Card, they may have"
            " the same, more, or even less properties than other various Card"
            " Data Modals &mdash; as such, they are documented individually in"
            " the following pages.\n"
        ),
    },
    # ── Booster group index ──────────────────────────────────────────
    {
        "slug": "booster",
        "title": "Booster",
        "description": (
            "A Booster is a data structure with containing property values of"
            " Data Models, and not a Data Model itself. The booster property"
            " is found on a Set Data Model."
        ),
        "keywords": "mtg, magic the gathering, mtgjson, json, booster",
        "body": (
            "A Booster is a data structure with containing property values of"
            " Booster configurations, and is not a Data Model itself.\n"
            "\n"
            "- **Parent model:** [Set](/data-models/set/)\n"
            "\n"
            "## Booster Configurations\n"
            "\n"
            "### Overview\n"
            "\n"
            "The actual Booster data is accessed through a property key that"
            " defines the [Booster Config](/data-models/booster/booster-config/)."
            " What this property key name is depends on the Set that contains"
            " this Data Model and all of its booster variations for that Set.\n"
            "\n"
            "### TypeScript Model\n"
            "\n"
            "::: details Toggle Model {open}\n"
            "\n"
            "```TypeScript\n"
            "{\n"
            "  data: Record<string, BoosterConfig>;\n"
            "}\n"
            "```\n"
            "\n"
            ":::\n"
            "\n"
            "The Data Models of a Booster configuration are documented in the"
            " following pages.\n"
        ),
    },
    # ── Price group index ────────────────────────────────────────────
    {
        "slug": "price",
        "title": "Price",
        "description": (
            "A Price is a data structure with containing property values of"
            " prices for a card, organized by its `uuid`, and is not a Data"
            " Model itself."
        ),
        "keywords": "mtg, magic the gathering, mtgjson, json, price list",
        "body": (
            "A Price is a data structure with containing property values of"
            " prices for a card, organized by its `uuid`, and is not a Data"
            " Model itself.\n"
            "\n"
            "- **Parent file:**"
            " [AllPrices](/downloads/all-files/#allprices),"
            " [AllPricesToday](/downloads/all-files/#allpricestoday)\n"
            "- **Parent property:** `data`\n"
            "\n"
            "### TypeScript Model\n"
            "\n"
            "::: details Toggle Model {open}\n"
            "\n"
            "```TypeScript\n"
            "{\n"
            "  data: Record<string, PriceFormats>;\n"
            "}\n"
            "```\n"
            "\n"
            ":::\n"
            "\n"
            "The Data Models of a configuration are documented in the"
            " following pages.\n"
        ),
    },
]
