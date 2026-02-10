"""
MTGJSON set models.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, Field

from .base import PolarsMixin
from .cards import CardSet, CardSetDeck, CardToken
from .sealed import SealedProduct
from .submodels import (
    BoosterConfig,
    Translations,
)

# =============================================================================
# Deck Models (minimal, for Set.decks)
# =============================================================================


class DeckSet(PolarsMixin, BaseModel):
    """Deck with minimal card references (as in Set.decks)."""

    model_config = {"populate_by_name": True}

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Deck (Set)"
    __doc_desc__: ClassVar[str] = (
        "The Deck (Set) Data Model describes the properties of an individual Deck within a [Set](/data-models/set/)."
    )
    __doc_parent__: ClassVar[str] = "**Parent model:** [Set](/data-models/set/)\n- **Parent property:** `decks`"
    __doc_enum__: ClassVar[str] = "deck"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, deck (set)"

    code: str = Field(
        description="The printing set code for the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    name: str = Field(
        description="The name of the deck.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    type: str = Field(
        description="The type of deck.",
        json_schema_extra={"introduced": "v4.3.0", "enum_key": "type"},
    )
    release_date: str | None = Field(
        default=None,
        alias="releaseDate",
        description=(
            "The release date in "
            "[ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) "
            "format for the set. Returns `null` if the deck was not formally released as a product."
        ),
        json_schema_extra={"introduced": "v4.3.0"},
    )
    sealed_product_uuids: list[str] | None = Field(
        default=None,
        alias="sealedProductUuids",
        description="A list of UUID's associated to this Deck in a [Sealed Product](/data-models/sealed-product/).",
        json_schema_extra={"introduced": "v5.2.2"},
    )
    main_board: list[CardSetDeck] = Field(
        default_factory=list,
        alias="mainBoard",
        description="The cards in the main-board. See the [Card (Set Deck)](/data-models/card/card-set-deck/) Data Model.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    side_board: list[CardSetDeck] = Field(
        default_factory=list,
        alias="sideBoard",
        description="The cards in the side-board. See the [Card (Set Deck)](/data-models/card/card-set-deck/) Data Model.",
        json_schema_extra={"introduced": "v4.3.0"},
    )
    commander: list[CardSetDeck] | None = Field(
        default=None,
        description="The card that is the Commander in this deck. See the [Card (Set Deck)](/data-models/card/card-set-deck/) Data Model.",
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )
    # Additional deck fields (not documented on website)
    display_commander: list[CardSetDeck] | None = Field(default=None, alias="displayCommander")
    tokens: list[CardSetDeck] | None = None
    planes: list[CardSetDeck] | None = None
    schemes: list[CardSetDeck] | None = None
    source_set_codes: list[str] | None = Field(default=None, alias="sourceSetCodes")


# =============================================================================
# Set Models
# =============================================================================


class SetList(PolarsMixin, BaseModel):
    """Set summary (without cards, for SetList.json)."""

    model_config = {"populate_by_name": True}

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Set List"
    __doc_desc__: ClassVar[str] = (
        "The Set List Data Model describes the meta data properties of an individual [Set](/data-models/set/)."
    )
    __doc_parent__: ClassVar[str] = (
        "**Parent file:** [SetList](/downloads/all-files/#setlist)\n- **Parent property:** `data`"
    )
    __doc_enum__: ClassVar[str] = "set"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, set list"

    code: str = Field(
        description="The printing set code for the set.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    name: str = Field(
        description="The name of the set.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    type: str = Field(
        description="The expansion type of the set.",
        json_schema_extra={"introduced": "v4.0.0", "enum_key": "type"},
    )
    release_date: str = Field(
        alias="releaseDate",
        description=(
            "The release date in [ISO 8601](https://www.iso.org/iso-8601-date-and-time-format.html) format for the set."
        ),
        json_schema_extra={"introduced": "v4.0.0"},
    )
    base_set_size: int = Field(
        alias="baseSetSize",
        description=(
            "The number of cards in the set. This will default to "
            "[totalSetSize](#totalsetsize) if not available. "
            "[Wizards of the Coast](https://company.wizards.com) sometimes prints "
            "extra cards beyond the set size into promos or supplemental products."
        ),
        json_schema_extra={"introduced": "v4.1.0"},
    )
    total_set_size: int = Field(
        alias="totalSetSize",
        description=("The total number of cards in the set, including promos and related supplemental products."),
        json_schema_extra={"introduced": "v4.1.0"},
    )
    keyrune_code: str = Field(
        alias="keyruneCode",
        description="The matching [Keyrune](https://keyrune.andrewgioia.com) code for set image icons.",
        json_schema_extra={"introduced": "v4.3.2"},
    )
    translations: Translations = Field(
        default_factory=dict,  # type: ignore[assignment]
        description="The translated set name by language. See the [Translations](/data-models/translations/) Data Model.",
        json_schema_extra={"introduced": "v4.3.2"},
    )

    # Optional fields
    block: str | None = Field(
        default=None,
        description="The block name the set is in.",
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    parent_code: str | None = Field(
        default=None,
        alias="parentCode",
        description="The parent printing set code for set variations like promotions, guild kits, etc.",
        json_schema_extra={"introduced": "v4.3.0", "optional": True},
    )
    mtgo_code: str | None = Field(
        default=None,
        alias="mtgoCode",
        description=(
            "The set code for the set as it appears on "
            "[Magic: The Gathering Online](https://magic.wizards.com/en/mtgo)."
        ),
        json_schema_extra={"introduced": "v4.0.0", "optional": True},
    )
    token_set_code: str | None = Field(
        default=None,
        alias="tokenSetCode",
        description="The tokens set code, formatted in uppercase.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    # External IDs
    mcm_id: int | None = Field(
        default=None,
        alias="mcmId",
        description=(
            "The [Cardmarket](https://www.cardmarket.com/en/Magic"
            "?utm_campaign=card_prices&utm_medium=text&utm_source=mtgjson) set identifier."
        ),
        json_schema_extra={"introduced": "v4.4.0", "optional": True},
    )
    mcm_id_extras: int | None = Field(
        default=None,
        alias="mcmIdExtras",
        description=(
            "The split [Cardmarket](https://www.cardmarket.com/en/Magic"
            "?utm_campaign=card_prices&utm_medium=text&utm_source=mtgjson) set identifier "
            "if a set is printed in two sets. This identifier represents the second set's identifier."
        ),
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )
    mcm_name: str | None = Field(
        default=None,
        alias="mcmName",
        description=(
            "The [Cardmarket](https://www.cardmarket.com/en/Magic"
            "?utm_campaign=card_prices&utm_medium=text&utm_source=mtgjson) set name."
        ),
        json_schema_extra={"introduced": "v4.4.0", "optional": True},
    )
    tcgplayer_group_id: int | None = Field(
        default=None,
        alias="tcgplayerGroupId",
        description=(
            "The group identifier of the set on "
            "[TCGplayer](https://www.tcgplayer.com?partner=mtgjson"
            "&utm_campaign=affiliate&utm_medium=mtgjson&utm_source=mtgjson)."
        ),
        json_schema_extra={"introduced": "v4.2.1", "optional": True},
    )
    cardsphere_set_id: int | None = Field(
        default=None,
        alias="cardsphereSetId",
        description="The [Cardsphere](https://www.cardsphere.com/) set identifier.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True},
    )

    # Flags
    is_foil_only: bool = Field(
        default=False,
        alias="isFoilOnly",
        description="If the set is only available in foil.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    is_non_foil_only: bool | None = Field(
        default=None,
        alias="isNonFoilOnly",
        description="If the set is only available in non-foil.",
        json_schema_extra={"introduced": "v5.0.0", "optional": True},
    )
    is_online_only: bool = Field(
        default=False,
        alias="isOnlineOnly",
        description="If the set is only available in online game play variations.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    is_paper_only: bool | None = Field(
        default=None,
        alias="isPaperOnly",
        description="If the set is only available in paper game play.",
        json_schema_extra={"introduced": "v4.6.2", "optional": True},
    )
    is_foreign_only: bool | None = Field(
        default=None,
        alias="isForeignOnly",
        description="If the set is only available outside the United States of America.",
        json_schema_extra={"introduced": "v4.4.1", "optional": True},
    )
    is_partial_preview: bool | None = Field(
        default=None,
        alias="isPartialPreview",
        description="If the set is still in preview (spoiled). Preview sets do not have complete data.",
        json_schema_extra={"introduced": "v4.4.2", "optional": True},
    )

    # Languages
    languages: list[str] | None = Field(
        default=None,
        description="The languages the set was printed in.",
        json_schema_extra={"introduced": "v5.2.1", "optional": True, "enum_key": "languages"},
    )

    # Decks and sealed products (included in SetList.json)
    decks: list[DeckSet] | None = Field(
        default=None,
        description="All decks associated to the set. See the [Deck (Set)](/data-models/deck-set/) Data Model.",
        json_schema_extra={"introduced": "v5.2.2", "optional": True},
    )
    sealed_product: list[SealedProduct] | None = Field(
        default=None,
        alias="sealedProduct",
        description="The sealed product information for the set. See the [Sealed Product](/data-models/sealed-product/) Data Model.",
        json_schema_extra={"introduced": "v5.1.0", "optional": True},
    )


class MtgSet(SetList):
    """Full set with cards, tokens, decks, etc."""

    __ts_name__: ClassVar[str] = "Set"

    # --- Doc generation metadata ---
    __doc_title__: ClassVar[str] = "Set"
    __doc_desc__: ClassVar[str] = "The Set Data Model describes the properties of an individual set."
    __doc_parent__: ClassVar[str] = (
        "**Parent file:** [AllIdentifiers](/downloads/all-files/#allidentifiers), "
        "[AllPrintings](/downloads/all-files/#allprintings), "
        "[Legacy](/downloads/all-files/#legacy), "
        "[Modern](/downloads/all-files/#modern), "
        "[Pioneer](/downloads/all-files/#pioneer), "
        "[Standard](/downloads/all-files/#standard), "
        "[Vintage](/downloads/all-files/#vintage)\n"
        "- **Parent property:** `data`"
    )
    __doc_enum__: ClassVar[str] = "set"
    __doc_keywords__: ClassVar[str] = "mtg, magic the gathering, mtgjson, json, set"

    # Overrides for inherited fields where Set page differs from SetList page
    __doc_field_overrides__: ClassVar[dict[str, dict[str, Any]]] = {
        "totalSetSize": {
            "description": (
                "The total number of cards in the set, including promotional and related "
                "supplemental products but excluding "
                "[Alchemy](https://magic.wizards.com/en/articles/archive/magic-digital/"
                "introducing-alchemy-new-way-play-mtg-arena-2021-12-02) modifications "
                "- however those cards are included in the set itself."
            ),
        },
        "sealedProduct": {
            "introduced": "v5.2.0",
        },
    }

    cards: list[CardSet] = Field(
        default_factory=list,
        description="The list of cards in the set. See the [Card (Set)](/data-models/card/card-set/) Data Model.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    tokens: list[CardToken] = Field(
        default_factory=list,
        description="The tokens cards in the set. See the [Card (Token)](/data-models/card/card-token/) Data Model.",
        json_schema_extra={"introduced": "v4.0.0"},
    )
    booster: dict[str, BoosterConfig] | None = Field(
        default=None,
        description=(
            "A breakdown of possibilities and weights of cards in a booster pack. "
            "See the [Booster Config](/data-models/booster/booster-config/) Data Model."
        ),
        json_schema_extra={"introduced": "v5.0.0", "optional": True, "type_override": "Record<string, BoosterConfig>"},
    )


# =============================================================================
# Namespace for Set Models
# =============================================================================
class Sets:
    """Namespace for all set models."""

    MtgSet = MtgSet
    DeckSet = DeckSet
    SetList = SetList


# =============================================================================
# Registry for TypeScript generation
# =============================================================================

SET_MODEL_REGISTRY: list[type[BaseModel]] = [
    DeckSet,
    SetList,
    MtgSet,
]

__all__ = [
    "SET_MODEL_REGISTRY",
    "Sets",
]
