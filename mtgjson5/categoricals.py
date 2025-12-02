"""Categorical column definitions for processing Scryfall data in Polars."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List

import polars as pl

if TYPE_CHECKING:
    from logging import Logger


@dataclass
class DynamicCategoricals:
    """Categorical values discovered from source data."""

    # From struct field introspection
    legalities: list[str] = field(default_factory=list)
    price_keys: list[str] = field(default_factory=list)
    image_uri_keys: list[str] = field(default_factory=list)
    purchase_uri_keys: list[str] = field(default_factory=list)
    related_uri_keys: list[str] = field(default_factory=list)
    # From list column unique values
    games: list[str] = field(default_factory=list)
    finishes: list[str] = field(default_factory=list)
    colors: list[str] = field(default_factory=list)
    promo_types: list[str] = field(default_factory=list)
    frame_effects: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    # From scalar column unique values
    rarities: list[str] = field(default_factory=list)
    border_colors: list[str] = field(default_factory=list)
    layouts: list[str] = field(default_factory=list)
    frames: list[str] = field(default_factory=list)
    security_stamps: list[str] = field(default_factory=list)
    set_types: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    image_statuses: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, int]:
        """Return counts for logging."""
        return {
            "legalities": len(self.legality_formats),
            "price_keys": len(self.price_keys),
            "games": len(self.games),
            "finishes": len(self.finishes),
            "colors": len(self.colors),
            "promo_types": len(self.promo_types),
            "frame_effects": len(self.frame_effects),
            "keywords": len(self.keywords),
            "rarities": len(self.rarities),
            "border_colors": len(self.border_colors),
            "layouts": len(self.layouts),
            "frames": len(self.frames),
            "security_stamps": len(self.security_stamps),
            "set_types": len(self.set_types),
            "languages": len(self.languages),
        }

    def _extract_struct_fields(self, schema: pl.Schema, col_name: str) -> list[str]:
        """Extract field names from a Struct column's schema."""
        if col_name not in schema.names():
            return []
        dtype = schema[col_name]
        if isinstance(dtype, pl.Struct):
            return sorted(f.name for f in dtype.fields)
        return []

    def _flatten_list_result(self, values: list) -> list[str]:
        """Flatten potentially nested list results from Polars aggregation."""
        if not values:
            return []
        # If first element is a list, we have nested structure from explode
        if isinstance(values[0], list):
            flat = set()
            for sublist in values:
                if sublist:
                    flat.update(v for v in sublist if v is not None)
            return sorted(flat)
        return sorted(v for v in values if v is not None)


def discover_categoricals(
    cards_lf: pl.LazyFrame,
    sets_lf: pl.LazyFrame | None = None,
    logger: "Logger | None" = None,
) -> "DynamicCategoricals":
    """
    Extract all categorical values from source data.
    """
    cats = DynamicCategoricals()
    schema = cards_lf.collect_schema()
    struct_mappings = {
        "legalities": "legality_formats",
        "prices": "price_keys",
        "image_uris": "image_uri_keys",
        "purchase_uris": "purchase_uri_keys",
        "related_uris": "related_uri_keys",
    }
    for col_name, attr in struct_mappings.items():
        fields = cats._extract_struct_fields(schema, col_name)
        if fields:
            setattr(cats, attr, fields)
            if logger:
                logger.debug(f"Discovered {len(fields)} {attr} from {col_name} struct")
    list_col_mappings = {
        "games": "games",
        "finishes": "finishes",
        "color_identity": "colors",
        "promo_types": "promo_types",
        "frame_effects": "frame_effects",
        "keywords": "keywords",
    }
    list_agg_exprs = []
    list_attrs = []
    for col, attr in list_col_mappings.items():
        if col in schema.names():
            # Explode, unique, then implode back to single-row list
            list_agg_exprs.append(
                pl.col(col)
                .explode()
                .drop_nulls()
                .unique()
                .sort()
                .implode()
                .alias(f"_cat_{attr}")
            )
            list_attrs.append(attr)
    scalar_col_mappings = {
        "rarity": "rarities",
        "border_color": "border_colors",
        "layout": "layouts",
        "frame": "frames",
        "security_stamp": "security_stamps",
        "set_type": "set_types",
        "lang": "languages",
        "image_status": "image_statuses",
    }
    scalar_agg_exprs = []
    scalar_attrs = []
    for col, attr in scalar_col_mappings.items():
        if col in schema.names():
            # Unique then implode to single-row list
            scalar_agg_exprs.append(
                pl.col(col).drop_nulls().unique().sort().implode().alias(f"_cat_{attr}")
            )
            scalar_attrs.append(attr)
    all_exprs = list_agg_exprs + scalar_agg_exprs
    all_attrs = list_attrs + scalar_attrs
    if all_exprs:
        if logger:
            logger.debug(f"  Scanning {len(all_exprs)} categorical columns...")
        result = cards_lf.select(all_exprs).collect()
        for attr in all_attrs:
            col_name = f"_cat_{attr}"
            if col_name in result.columns:
                # Each column is a single-element list column, extract the list
                values = result[col_name].to_list()[0]
                if values:
                    setattr(cats, attr, list(values))
    if sets_lf is not None and "set_type" in sets_lf.collect_schema().names():
        set_types = (
            sets_lf.select(pl.col("set_type").drop_nulls().unique())
            .collect()
            .to_series()
            .to_list()
        )
        cats.set_types = sorted(set(cats.set_types) | set(set_types))
    if logger:
        summary = cats.summary()
        total = sum(summary.values())
        logger.info(
            f"  Discovered {total} categorical values across {len(summary)} categories"
        )
    return cats


def log_categoricals_diff(
    current: DynamicCategoricals,
    previous: DynamicCategoricals | None,
    logger: "Logger",
) -> None:
    """
    Log differences between current and previous categorical values.

    Useful for detecting when Scryfall adds new formats, layouts, etc.
    """
    if previous is None:
        return

    attrs = [
        "legalities",
        "games",
        "finishes",
        "layouts",
        "rarities",
        "security_stamps",
        "set_types",
    ]

    for attr in attrs:
        current_vals = set(getattr(current, attr))
        previous_vals = set(getattr(previous, attr))

        added = current_vals - previous_vals
        removed = previous_vals - current_vals

        if added:
            logger.info(f"  New {attr}: {sorted(added)}")
        if removed:
            logger.warning(f"  Removed {attr}: {sorted(removed)}")


# These require .list.eval() for casting
LIST_CATEGORICAL_COLS = [
    "colors",
    "color_identity",
    "color_indicator",
    "produced_mana",
    "frame_effects",
    "finishes",
    "games",
    "promo_types",
    "keywords",
]

# Columns that are simple strings (Utf8)
SCALAR_CATEGORICAL_COLS = [
    "rarity",
    "layout",
    "border_color",
    "frame",
    "security_stamp",
    "set_type",
    "image_status",
    "lang",
]

# This enables O(1) integer comparisons instead of string comparisons
STATIC_CATEGORICALS: Dict[str, List[str]] = {
    "colors": ["W", "U", "B", "R", "G"],
    "color_identity": ["W", "U", "B", "R", "G"],
    "color_indicator": ["W", "U", "B", "R", "G"],
    "rarity": ["common", "uncommon", "rare", "mythic", "special", "bonus"],
    "layout": [
        "normal",
        "split",
        "flip",
        "transform",
        "modal_dfc",
        "meld",
        "leveler",
        "class",
        "case",
        "saga",
        "adventure",
        "mutate",
        "prototype",
        "battle",
        "planar",
        "scheme",
        "vanguard",
        "token",
        "double_faced_token",
        "emblem",
        "augment",
        "host",
        "art_series",
        "reversible_card",
    ],
    "border_color": ["black", "white", "borderless", "silver", "gold"],
    "frame": ["1993", "1997", "2003", "2015", "future"],
    "frame_effects": [
        "legendary",
        "miracle",
        "enchantment",
        "draft",
        "devoid",
        "tombstone",
        "colorshifted",
        "inverted",
        "sunmoondfc",
        "compasslanddfc",
        "originpwdfc",
        "mooneldrazidfc",
        "waxingandwaningmoondfc",
        "showcase",
        "extendedart",
        "companion",
        "etched",
        "snow",
        "lesson",
        "shatteredglass",
        "convertdfc",
        "fandfc",
        "upsidedowndfc",
        "spree",
    ],
    "security_stamp": ["oval", "triangle", "acorn", "circle", "arena", "heart"],
    "lang": [
        "en",
        "es",
        "fr",
        "de",
        "it",
        "pt",
        "ja",
        "ko",
        "ru",
        "zhs",
        "zht",
        "he",
        "la",
        "grc",
        "ar",
        "sa",
        "ph",
    ],
    "finishes": ["foil", "nonfoil", "etched", "glossy"],
    "games": ["paper", "arena", "mtgo", "astral", "sega"],
    "image_status": ["missing", "placeholder", "lowres", "highres_scan"],
    "set_type": [
        "core",
        "expansion",
        "masters",
        "alchemy",
        "masterpiece",
        "arsenal",
        "from_the_vault",
        "spellbook",
        "premium_deck",
        "duel_deck",
        "draft_innovation",
        "treasure_chest",
        "commander",
        "planechase",
        "archenemy",
        "vanguard",
        "funny",
        "starter",
        "box",
        "promo",
        "token",
        "memorabilia",
        "minigame",
    ],
    "legalities": [
        "alchemy",
        "brawl",
        "commander",
        "duel",
        "explorer",
        "future",
        "gladiator",
        "historic",
        "historicbrawl",
        "legacy",
        "modern",
        "oathbreaker",
        "oldschool",
        "pauper",
        "paupercommander",
        "penny",
        "pioneer",
        "predh",
        "premodern",
        "standard",
        "standardbrawl",
        "timeless",
        "vintage",
    ],
}
