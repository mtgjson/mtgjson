"""
MTGJSON card models.

Inheritance hierarchy:
- CardBase: Common fields for all cards
  - CardAtomicBase: Oracle/evergreen properties
    - CardAtomic: Final atomic card model
  - CardPrintingBase: Printing-specific properties
    - CardPrintingFull: Full printing (atomic + printing)
      - CardSet: Card in a set
      - CardDeck: Card in a deck (with count/foil)
    - CardToken: Token card
- CardSetDeck: Minimal card reference in deck
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, Field

from .base import PolarsMixin
from .submodels import (
    ForeignData,
    Identifiers,
    LeadershipSkills,
    Legalities,
    PurchaseUrls,
    RelatedCards,
    Rulings,
    SourceProducts,
)

# Layouts that use WUBRG color order instead of alphabetical
_WUBRG_COLOR_LAYOUTS = frozenset({"split", "adventure"})
_WUBRG_ORDER = {"W": 0, "U": 1, "B": 2, "R": 3, "G": 4}


class CardBase(PolarsMixin, BaseModel):
    """Base fields shared by all card types."""

    model_config = {"populate_by_name": True}

    # Identity
    name: str = Field(
        description="The name of the card. Cards with multiple faces are given a `//` delimiter.",
        json_schema_extra={"introduced": "v4.0.0", "example": '"Wear // Tear"'},
    )
    ascii_name: str | None = Field(
        default=None,
        alias="asciiName",
        description=(
            "The [ASCII](http://www.asciitable.com) (Basic/128) code formatted card name "
            "with no special unicode characters."
        ),
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )
    face_name: str | None = Field(
        default=None,
        alias="faceName",
        description="The name on the face of the card.",
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )

    # Type line
    type: str = Field(
        description=(
            "The type of the card as visible, including any supertypes and subtypes "
            "and are given a `-` delimiter if appropriate."
        ),
        json_schema_extra={"introduced": "v4.0.0"},
    )
    types: list[str] = Field(
        default_factory=list,
        description="A list of all card types of the card, including Un\u2011sets and gameplay variants.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "types"},
    )
    subtypes: list[str] = Field(
        default_factory=list,
        description="A list of card subtypes found after em-dash.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "subtypes"},
    )
    supertypes: list[str] = Field(
        default_factory=list,
        description="A list of card supertypes found before em-dash.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "supertypes"},
    )

    # Colors
    colors: list[str] = Field(
        default_factory=list,
        description=(
            "A list of all the colors in `manaCost` and `colorIndicator` properties. "
            'Some cards may not have values, such as cards with `"Devoid"` in its [text](#text).'
        ),
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "colors"},
    )
    color_identity: list[str] = Field(
        default_factory=list,
        alias="colorIdentity",
        description="A list of all the colors found in `manaCost`, `colorIndicator`, and `text` properties.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "colorIdentity"},
    )
    color_indicator: list[str] | None = Field(
        default=None,
        alias="colorIndicator",
        description="A list of all the colors in the color indicator. This is the symbol prefixed to a card's [types](#types).",
        json_schema_extra={"introduced": "v4.0.2", "optional": True, "enum_key": "colorIndicator"},
    )
    produced_mana: list[str] | None = Field(default=None, alias="producedMana")

    # Mana
    mana_cost: str | None = Field(
        default=None,
        alias="manaCost",
        description="The mana cost of the card wrapped in curly brackets for each mana symbol value.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True, "example": '"{1}{B}"'},
    )

    # Text
    text: str | None = Field(
        default=None,
        description="The rules text of the card.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Layout
    layout: str = Field(
        description="The type of card layout.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "layout"},
    )
    side: str | None = Field(
        default=None,
        description="The identifier of the card side. Used on cards with multiple faces on the same card.",
        json_schema_extra={"introduced": "v4.1.0", "optional": True, "enum_key": "side"},
    )

    # Stats
    power: str | None = Field(
        default=None,
        description="The power of the card.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    toughness: str | None = Field(
        default=None,
        description="The toughness of the card.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    loyalty: str | None = Field(
        default=None,
        description='The starting loyalty value of the card. Used only on cards with `"Planeswalker"` in its [types](./#types).',
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Keywords
    keywords: list[str] | None = Field(
        default=None,
        description="A list of keywords found on the card.",
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )

    # Identifiers
    identifiers: Identifiers = Field(
        default_factory=dict,  # type: ignore[assignment]
        description="The identifiers associated to a card. See the [Identifiers](/data-models/identifiers/) Data Model.",
        json_schema_extra={"introduced": "v5.0.0"},
    )

    # Flags
    is_funny: bool | None = Field(
        default=None,
        alias="isFunny",
        description="If the card is part of a funny set, such as an Un-set.",
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )

    # EDHREC
    edhrec_saltiness: float | None = Field(
        default=None,
        alias="edhrecSaltiness",
        description="The card saltiness score on [EDHRec](https://www.edhrec.com).",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    # Subsets
    subsets: list[str] | None = Field(
        default=None,
        description='The names of the subset printings a card is in. Used primarily on "Secret Lair Drop" cards.',
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    def to_polars_dict(
        self,
        use_alias: bool = True,
        sort_keys: bool = True,
        sort_lists: bool = True,
        exclude_none: bool = False,
        keep_empty_lists: bool = False,
    ) -> dict[str, Any]:
        """Convert to dict, preserving WUBRG color order for split/adventure layouts."""
        result = super().to_polars_dict(use_alias, sort_keys, sort_lists, exclude_none, keep_empty_lists)

        if self.layout in _WUBRG_COLOR_LAYOUTS and "colors" in result and result["colors"]:
            result["colors"] = sorted(result["colors"], key=lambda c: _WUBRG_ORDER.get(c, 99))

        return result


class CardAtomicBase(CardBase):
    """Base for atomic (oracle-like) card properties."""

    # Mana values
    converted_mana_cost: float = Field(
        alias="convertedManaCost",
        description="The converted mana cost of the card.",
        json_schema_extra={
            "introduced": "v4.0.0",
            "deprecated": True,
            "deprecated_msg": "This property is deprecated. Use the [manaValue](#manavalue) property instead.",
        },
    )
    mana_value: float = Field(
        alias="manaValue",
        description="The mana value of the card.",
        json_schema_extra={"introduced": "v5.2.0"},
    )
    face_converted_mana_cost: float | None = Field(
        default=None,
        alias="faceConvertedManaCost",
        description="The converted mana cost or mana value for the face for either half or part of the card.",
        json_schema_extra={
            "introduced": "v4.1.1",
            "optional": True,
            "deprecated": True,
            "deprecated_msg": "This property is deprecated. Use the [faceManaValue](#facemanavalue) property instead.",
        },
    )
    face_mana_value: float | None = Field(
        default=None,
        alias="faceManaValue",
        description="The mana value of the face for either half or part of the card.",
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )

    # Battle
    defense: str | None = Field(
        default=None,
        description="The defense of the card. Used on [battle](https://mtg.wiki/page/Battle) cards.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    # Vanguard
    hand: str | None = Field(
        default=None,
        description=(
            "The starting maximum hand size total modifier. A `+` or `-` character precedes a number. "
            'Used only on cards with `"Vanguard"` in its [types](./#types).'
        ),
        json_schema_extra={"introduced": "v4.2.1", "optional": True},
    )
    life: str | None = Field(
        default=None,
        description=(
            "The starting life total modifier. A `+` or `-` character precedes a number. "
            'Used only on cards with `"Vanguard"` in its [types](./#types).'
        ),
        json_schema_extra={"introduced": "v4.2.1", "optional": True},
    )

    # EDHREC
    edhrec_rank: int | None = Field(
        default=None,
        alias="edhrecRank",
        description="The card rank on [EDHRec](https://www.edhrec.com).",
        json_schema_extra={"introduced": "v4.5.0", "optional": True},
    )

    # Foreign
    foreign_data: list[ForeignData] | None = Field(
        default=None,
        alias="foreignData",
        description="A list of data properties in other languages. See the [Foreign Data](/data-models/foreign-data/) Data Model.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Rules
    legalities: Legalities = Field(
        default_factory=dict,  # type: ignore[assignment]
        description="The legalities of play formats for this printing of the card. See the [Legalities](/data-models/legalities/) Data Model.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    leadership_skills: LeadershipSkills | None = Field(
        default=None,
        alias="leadershipSkills",
        description="The formats the card is legal to be a commander in. See the [Leadership Skills](/data-models/leadership-skills/) Data Model.",
        json_schema_extra={"introduced": "v4.5.1", "optional": True},
    )
    rulings: list[Rulings] | None = Field(
        default=None,
        description="A list of the official rulings of the card. See the [Rulings](/data-models/rulings/) Data Model.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Deck limits
    has_alternative_deck_limit: bool | None = Field(
        default=None,
        alias="hasAlternativeDeckLimit",
        description="If the card allows a value other than 4 copies in a deck.",
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )

    # Reserved
    is_reserved: bool | None = Field(
        default=None,
        alias="isReserved",
        description=(
            "If the card is on the Magic: The Gathering "
            "[Reserved List](https://magic.wizards.com/en/articles/archive/official-reprint-policy-2010-03-10)."
        ),
        json_schema_extra={"introduced": "v4.0.1", "optional": True},
    )
    is_game_changer: bool | None = Field(
        default=None,
        alias="isGameChanger",
        description="If the card is a part of the [game changers](https://mtg.wiki/page/Game_Changers) commander list.",
        json_schema_extra={"introduced": "v5.2.2", "optional": True},
    )

    # Printings
    printings: list[str] | None = Field(
        default=None,
        description="A list of printing set codes the card was printed in, formatted in uppercase.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Purchase
    purchase_urls: PurchaseUrls | None = Field(
        default=None,
        alias="purchaseUrls",
        description="Links that navigate to websites where the card can be purchased. See the [Purchase Urls](/data-models/purchase-urls/) Data Model.",
        json_schema_extra={"introduced": "v4.4.0"},
    )
    related_cards: RelatedCards | None = Field(
        default=None,
        alias="relatedCards",
        description="The related cards for this card. See the [Related Cards](/data-models/related-cards/) Data Model.",
        json_schema_extra={"introduced": "v5.2.1"},
    )


class CardPrintingBase(CardBase):
    """Base for printing-specific card properties."""

    # Printing identity
    uuid: str = Field(
        description="The universal unique identifier (v5) generated by MTGJSON.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    set_code: str = Field(
        alias="setCode",
        description="The printing set code that the card is from, formatted in uppercase.",
        json_schema_extra={"introduced": "v5.0.1"},
    )
    number: str = Field(
        description="The number of the card. Cards can have a variety of numbers, letters and/or symbols for promotional qualities.",
        json_schema_extra={"introduced": "v4.0.0"},
    )

    def __lt__(self, other: object) -> bool:
        """Sort by collector number, then side.

        Mirrors MtgjsonCardObject.__lt__ logic for consistency.
        """
        if not isinstance(other, CardPrintingBase):
            return NotImplemented

        self_side = self.side or ""
        other_side = other.side or ""

        if self.number == other.number:
            return self_side < other_side

        self_number_clean = "".join(x for x in self.number if x.isdigit()) or "100000"
        self_number_clean_int = int(self_number_clean)

        other_number_clean = "".join(x for x in other.number if x.isdigit()) or "100000"
        other_number_clean_int = int(other_number_clean)

        if self.number == self_number_clean and other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                if len(self_number_clean) != len(other_number_clean):
                    return len(self_number_clean) < len(other_number_clean)
                return self_side < other_side
            return self_number_clean_int < other_number_clean_int

        if self.number == self_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return True
            return self_number_clean_int < other_number_clean_int

        if other.number == other_number_clean:
            if self_number_clean_int == other_number_clean_int:
                return False
            return self_number_clean_int < other_number_clean_int

        if self_number_clean == other_number_clean:
            # Same numeric part - compare by side first
            if self_side != other_side:
                return self_side < other_side
            # Tiebreaker: multi-face cards sort by name, then by collector number
            if self_side and self.name != other.name:
                return self.name < other.name
            return bool(self.number < other.number)

        if self_number_clean_int == other_number_clean_int:
            if len(self_number_clean) != len(other_number_clean):
                return len(self_number_clean) < len(other_number_clean)
            if self_side != other_side:
                return self_side < other_side
            # Tiebreaker: multi-face cards sort by name, then by collector number
            if self_side and self.name != other.name:
                return self.name < other.name
            return bool(self.number < other.number)

        return self_number_clean_int < other_number_clean_int

    # Visual
    artist: str | None = Field(
        default=None,
        description="The name of the artist that illustrated the card art.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    artist_ids: list[str] | None = Field(
        default=None,
        alias="artistIds",
        description="A list of identifiers for the artists that illustrated the card art.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )
    border_color: str = Field(
        alias="borderColor",
        description="The color of the card border.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "borderColor"},
    )
    frame_version: str = Field(
        alias="frameVersion",
        description="The version of the card frame style.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "frameVersion"},
    )
    frame_effects: list[str] | None = Field(
        default=None,
        alias="frameEffects",
        description="The visual frame effects.",
        json_schema_extra={"introduced": "v4.6.0", "optional": True, "enum_key": "frameEffects"},
    )
    watermark: str | None = Field(
        default=None,
        description="The name of the watermark on the card.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True, "enum_key": "watermark"},
    )
    signature: str | None = Field(
        default=None,
        description="The name of the signature on the card.",
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )
    security_stamp: str | None = Field(
        default=None,
        alias="securityStamp",
        description="The security stamp printed on the card.",
        json_schema_extra={"introduced": "v5.2.0", "optional": True, "enum_key": "securityStamp"},
    )

    # Flavor
    flavor_text: str | None = Field(
        default=None,
        alias="flavorText",
        description="The italicized text found below the rules text that has no game function.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    flavor_name: str | None = Field(
        default=None,
        alias="flavorName",
        description=(
            "The promotional card name printed above the true card name on special cards that has no game function. "
            "See [this card](https://scryfall.com/card/plg20/2/hangarback-walker) for an example."
        ),
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )
    face_flavor_name: str | None = Field(
        default=None,
        alias="faceFlavorName",
        description="The flavor name on the face of the card.",
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )

    # Original text (Gatherer)
    original_text: str | None = Field(
        default=None,
        alias="originalText",
        description="The text on the card as originally printed.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    original_type: str | None = Field(
        default=None,
        alias="originalType",
        description="The type of the card as originally printed. Includes any supertypes and subtypes.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )

    # Printed text (localized content for non-English cards) - not documented on website
    printed_name: str | None = Field(default=None, alias="printedName")
    printed_text: str | None = Field(default=None, alias="printedText")
    printed_type: str | None = Field(default=None, alias="printedType")
    face_printed_name: str | None = Field(default=None, alias="facePrintedName")

    # Availability
    availability: list[str] = Field(
        default_factory=list,
        description="A list of the card's available printing types.",
        json_schema_extra={"introduced": "v5.0.0", "enum_key": "availability"},
    )
    booster_types: list[str] | None = Field(
        default=None,
        alias="boosterTypes",
        description="A list of types this card is in a booster pack.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True, "enum_key": "boosterTypes"},
    )
    finishes: list[str] = Field(
        default_factory=list,
        description="The finishes of the card. These finishes are not mutually exclusive.",
        json_schema_extra={"introduced": "v5.2.0", "enum_key": "finishes"},
    )
    
    promo_types: list[str] | None = Field(
        default=None,
        alias="promoTypes",
        description="A list of promotional types for a card.",
        json_schema_extra={"introduced": "v5.0.0", "optional": True, "enum_key": "promoTypes"},
    )

    # Un-sets
    attraction_lights: list[int] | None = Field(
        default=None,
        alias="attractionLights",
        description="A list of attraction lights found on a card, available only to cards printed in certain Un-sets.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    # Flags
    is_full_art: bool | None = Field(
        default=None,
        alias="isFullArt",
        description="If the card has full artwork.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )
    is_online_only: bool | None = Field(
        default=None,
        alias="isOnlineOnly",
        description="If the card is only available in online game play variations.",
        json_schema_extra={"introduced": "v4.0.1", "optional": True},
    )
    is_oversized: bool | None = Field(
        default=None,
        alias="isOversized",
        description="If the card is oversized.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    is_promo: bool | None = Field(
        default=None,
        alias="isPromo",
        description="If the card is a promotional printing.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )
    is_reprint: bool | None = Field(
        default=None,
        alias="isReprint",
        description="If the card has been reprinted.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )
    is_textless: bool | None = Field(
        default=None,
        alias="isTextless",
        description="If the card does not have a text box.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )

    # Multi-face
    other_face_ids: list[str] | None = Field(
        default=None,
        alias="otherFaceIds",
        description="A list of card `uuid`'s to this card's counterparts, such as transformed or melded faces.",
        json_schema_extra={"introduced": "v4.6.1", "optional": True},
    )
    card_parts: list[str] | None = Field(
        default=None,
        alias="cardParts",
        description='A list of card names associated to this card, such as `"Meld"` card face names.',
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )

    # Language
    language: str = Field(
        default="English",
        description="The language the card is printed in.",
        json_schema_extra={"introduced": "v5.2.1", "enum_key": "language"},
    )

    # Source
    source_products: list[str] | None = Field(default=None, alias="sourceProducts")


class CardPrintingFull(CardPrintingBase, CardAtomicBase):
    """Full printing with all atomic + printing fields."""

    # Rarity
    rarity: str = Field(
        description=(
            "The card printing rarity. Rarity `bonus` relates to cards that have an alternate availability "
            'in booster packs, while `special` relates to "Timeshifted" cards.'
        ),
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "rarity"},
    )

    # Duel deck
    duel_deck: str | None = Field(
        default=None,
        alias="duelDeck",
        description="The indicator for which duel deck the card is in.",
        json_schema_extra={"introduced": "v4.2.0", "optional": True, "enum_key": "duelDeck"},
    )

    # Rebalanced
    is_rebalanced: bool | None = Field(
        default=None,
        alias="isRebalanced",
        description=(
            "If the card is [rebalanced](https://magic.wizards.com/en/articles/archive/magic-digital/"
            "alchemy-rebalancing-philosophy-2021-12-02) for the "
            "[Alchemy](https://magic.wizards.com/en/articles/archive/magic-digital/"
            "introducing-alchemy-new-way-play-mtg-arena-2021-12-02) play format."
        ),
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )
    original_printings: list[str] | None = Field(
        default=None,
        alias="originalPrintings",
        description=(
            "A list of card `uuid`'s to original printings of the card if this card is somehow different "
            "from its original, such as [rebalanced](https://magic.wizards.com/en/articles/archive/"
            "magic-digital/alchemy-rebalancing-philosophy-2021-12-02) cards."
        ),
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )
    rebalanced_printings: list[str] | None = Field(
        default=None,
        alias="rebalancedPrintings",
        description=(
            "A list of card `uuid`'s to printings that are "
            "[rebalanced](https://magic.wizards.com/en/articles/archive/magic-digital/"
            "alchemy-rebalancing-philosophy-2021-12-02) versions of this card."
        ),
        json_schema_extra={"introduced": "v5.2.0", "optional": True},
    )
    original_release_date: str | None = Field(
        default=None,
        alias="originalReleaseDate",
        description=(
            "The original release date in "
            "[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format "
            "for a promotional card printed outside of a cycle window, such as Secret Lair Drop promotions."
        ),
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )

    # Flags
    is_alternative: bool | None = Field(
        default=None,
        alias="isAlternative",
        description="If the card is an alternate variation to a printing in this set.",
        json_schema_extra={"introduced": "v4.2.0", "optional": True},
    )
    is_story_spotlight: bool | None = Field(
        default=None,
        alias="isStorySpotlight",
        description="If the card is a [Story Spotlight](https://mtg.wiki/page/Story_Spotlight) card.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )
    is_timeshifted: bool | None = Field(
        default=None,
        alias="isTimeshifted",
        description=(
            'If the card is "timeshifted", a feature of certain sets where a card will have '
            "a different [frameVersion](#frameversion)."
        ),
        json_schema_extra={"introduced": "v4.4.1", "optional": True},
    )
    has_content_warning: bool | None = Field(
        default=None,
        alias="hasContentWarning",
        description=(
            "If the card marked by [Wizards of the Coast](https://company.wizards.com) for having sensitive content. "
            "Cards with this property may have missing or degraded properties. See this "
            "[official article](https://magic.wizards.com/en/articles/archive/news/depictions-racism-magic-2020-06-10) "
            "for more information."
        ),
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )

    # Variations
    variations: list[str] | None = Field(
        default=None,
        description="A list of card `uuid`'s of this card with alternate printings in the same set, excluding Un\u2011sets.",
        json_schema_extra={"introduced": "v4.1.2", "optional": True},
    )


# =============================================================================
# Final Card Models
# =============================================================================


class CardAtomic(CardAtomicBase):
    """Oracle-like card with evergreen properties (no printing info)."""

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Card (Atomic)"
    __doc_slug__: ClassVar[str] = "card/card-atomic"
    __doc_desc__: ClassVar[str] = (
        'The Card (Atomic) Data Model describes the properties of a single "atomic" card, '
        "an oracle-like entity of a card that only has evergreen properties that would never "
        "change from printing to printing."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent file:** [AtomicCards](/downloads/all-files/#atomiccards), "
        "[LegacyAtomic](/downloads/all-files/#legacyatomic), "
        "[ModernAtomic](/downloads/all-files/#modernatomic), "
        "[PauperAtomic](/downloads/all-files/#pauperatomic), "
        "[PioneerAtomic](/downloads/all-files/#pioneeratomic), "
        "[StandardAtomic](/downloads/all-files/#standardatomic), "
        "[VintageAtomic](/downloads/all-files/#vintageatomic)\n"
        "- **Parent property:** `data`"
    )
    __doc_enum__: ClassVar[str] = "card"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, card, card atomic"
    __doc_extra__: ClassVar[str] = (
        "::: tip Accessing Card (Atomic) Data\n\n"
        "When using any **Atomic-like** file, the Card (Atomic) Data Model is accessed through "
        "a single index array where its parent property is the card's "
        "[name](/data-models/card/card-atomic/#name) property.\n\n"
        "```TypeScript\n"
        "{\n"
        "  data: Record<string, CardAtomic[]>;\n"
        "}\n"
        "```\n\n"
        ":::"
    )

    # Overrides for inherited fields where CardAtomic differs
    __doc_field_overrides__: ClassVar[dict[str, dict[str, Any]]] = {
        "asciiName": {"introduced": "v5.1.0"},
        "keywords": {"introduced": "v5.1.0"},
        "identifiers": {"introduced": "v5.1.0"},
        "isFunny": {"introduced": "v5.2.1"},
        "foreignData": {"introduced": "v5.2.1"},
    }

    first_printing: str | None = Field(
        default=None,
        alias="firstPrinting",
        description="The set code the card was first printed in.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )


class CardSet(CardPrintingFull):
    """Card as it appears in a set."""

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Card (Set)"
    __doc_slug__: ClassVar[str] = "card/card-set"
    __doc_desc__: ClassVar[str] = (
        "The Card (Set) Data Model describes the properties of a single card found in a [Set](/data-models/set/)."
    )
    __doc_parent__: ClassVar[str] = "**Parent model:** [Set](/data-models/set/)\n- **Parent property:** `cards`"
    __doc_enum__: ClassVar[str] = "card"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, card, card set"

    source_products: SourceProducts | None = Field(  # type: ignore
        default=None,
        alias="sourceProducts",
        description=(
            "The source product identifiers linked to a [Sealed Product](/data-models/sealed-product/). "
            "See the [Source Products](/data-models/source-products/) Data Model."
        ),
        json_schema_extra={"introduced": "v5.2.2", "optional": True},
    )


class CardDeck(CardPrintingFull):
    """Card in a deck with count and foil info."""

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Card (Deck)"
    __doc_slug__: ClassVar[str] = "card/card-deck"
    __doc_desc__: ClassVar[str] = (
        "The Card (Deck) Data Model describes the properties of a single card found in a [Deck](/data-models/deck/)."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent model:** [Deck](/data-models/deck/)\n- **Parent property:** `commander`, `mainBoard`, `sideBoard`"
    )
    __doc_enum__: ClassVar[str] = "card"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, card, card deck"

    count: int = Field(
        description="The count of how many of this card exists in a relevant deck.",
        json_schema_extra={"introduced": "v4.4.1"},
    )
    is_foil: bool | None = Field(
        default=None,
        alias="isFoil",
        description="If the card is in foil.",
        json_schema_extra={"introduced": "v5.0.0"},
    )
    is_etched: bool | None = Field(default=None, alias="isEtched")
    source_products: SourceProducts | None = Field(  # type: ignore
        default=None,
        alias="sourceProducts",
        description="A list of associated Sealed Product `uuid` properties where this card can be found in.",
        json_schema_extra={"introduced": "v5.2.2", "optional": True, "type_override": "string[]"},
    )
    original_release_date: str | None = Field(
        default=None,
        alias="originalReleaseDate",
        description=(
            "The original release date in "
            "[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format "
            "for a promotional card printed outside of a cycle window, such as Secret Lair Drop promotions."
        ),
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )


class CardToken(CardPrintingBase):
    """Token card."""

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Card (Token)"
    __doc_slug__: ClassVar[str] = "card/card-token"
    __doc_desc__: ClassVar[str] = (
        "The Card (Token) Data Model describes the properties of a single token card found in a [Set](/data-models/set/)."
    )
    __doc_parent__: ClassVar[str] = "**Parent model:** [Set](/data-models/set/)\n- **Parent property:** `tokens`"
    __doc_enum__: ClassVar[str] = "card"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, card, card token"

    # Overrides for inherited fields where CardToken differs
    __doc_field_overrides__: ClassVar[dict[str, dict[str, Any]]] = {
        "manaCost": {"introduced": "v5.2.2"},
        "isOversized": {"introduced": "v5.2.2"},
        "flavorName": {"introduced": "v5.2.2"},
        "layout": {
            "description": 'The type of card layout. For a Card (Token), this will only ever be `"token"`.',
        },
    }

    orientation: str | None = Field(
        default=None,
        description="The layout direction of the card. Used on [Art cards](https://mtg.wiki/page/Art_card).",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )
    reverse_related: list[str] | None = Field(
        default=None,
        alias="reverseRelated",
        description="The names of the cards that produce this card.",
        json_schema_extra={
            "introduced": "v4.0.0",
            "optional": True,
            "deprecated": True,
            "deprecated_msg": "This property is deprecated. Use the [relatedCards](#relatedcards) property instead.",
        },
    )
    related_cards: RelatedCards | None = Field(
        default=None,
        alias="relatedCards",
        description="The related cards for this card. See the [Related Cards](/data-models/related-cards/) Data Model.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )
    edhrec_saltiness: float | None = Field(
        default=None,
        alias="edhrecSaltiness",
        description="The card saltiness score on [EDHRec](https://www.edhrec.com).",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )
    source_products: SourceProducts | None = Field(  # type: ignore
        default=None,
        alias="sourceProducts",
        description="A list of associated Sealed Product `uuid` properties where this card can be found in.",
        json_schema_extra={"introduced": "v5.2.2", "optional": True, "type_override": "string[]"},
    )
    token_products: list[Any] | None = Field(default=None, alias="tokenProducts")


class CardSetDeck(PolarsMixin, BaseModel):
    """Minimal card reference in a deck (used in Set.decks)."""

    model_config = {"populate_by_name": True}

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Card (Set Deck)"
    __doc_slug__: ClassVar[str] = "card/card-set-deck"
    __doc_desc__: ClassVar[str] = (
        "The Card (Set Deck) Data Model describes the properties of a single card found in "
        "a [Deck (Set)](/data-models/deck-set/)."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent model:** [Deck (Set)](/data-models/deck-set/)\n- **Parent property:** `cards`"
    )
    __doc_enum__: ClassVar[str] = "card"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, card, card deck meta"

    count: int = Field(
        description="The amount of this cards in the deck.",
        json_schema_extra={"introduced": "v5.2.2"},
    )
    is_foil: bool | None = Field(
        default=None,
        alias="isFoil",
        description="If the card is foiled.",
        json_schema_extra={"introduced": "v5.2.2", "optional": True},
    )
    uuid: str = Field(
        description="The universal unique identifier (v5) generated by MTGJSON.",
        json_schema_extra={"introduced": "v5.2.2"},
    )


class Cards:
    """Namespace for all card models."""

    CardAtomic = CardAtomic
    CardSet = CardSet
    CardDeck = CardDeck
    CardToken = CardToken
    CardSetDeck = CardSetDeck


CARD_MODEL_REGISTRY: list[type[BaseModel]] = [
    CardSetDeck,
    CardToken,
    CardAtomic,
    CardSet,
    CardDeck,
]

__all__ = [
    "CARD_MODEL_REGISTRY",
    "Cards",
]
