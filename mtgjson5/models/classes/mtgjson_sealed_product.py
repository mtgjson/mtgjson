"""MTGJSON Sealed Product Object model for booster boxes and sealed items."""

import enum
import uuid
from typing import Any

from pydantic import Field, model_validator

from ..mtgjson_base import MTGJsonModel
from .mtgjson_identifiers import MtgjsonIdentifiersObject
from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject


class MtgjsonSealedProductCategory(enum.Enum):
    """
    MTGJSON Sealed Product Category
    """

    # Unknown
    UNKNOWN = None

    # Packs
    BOOSTER_PACK = "booster_pack"

    # A Box containing boosters
    BOOSTER_BOX = "booster_box"

    # A box containing a variable amount of booster boxes
    BOOSTER_CASE = "booster_case"

    # Decks
    DECK = "deck"

    # Multi-Deck product: such as 2 player starter sets and game night
    MULTI_DECK = "multiple_decks"

    # A Display box containing decks
    DECK_BOX = "deck_box"

    # A box containing a known list of cards
    BOX_SET = "box_set"

    # A kit of (usually) predetermined cards for deck building
    KIT = "kit"

    # A boxed group of boosters, typically with a dice
    BUNDLE = "bundle"

    # A case of bundles
    BUNDLE_CASE = "bundle_case"

    # A prebuilt limited play pack, such as a prerelease kit or draft kit
    LIMITED = "limited_aid_tool"

    # A case of limited aid tools
    LIMITED_CASE = "limited_aid_case"

    # Set of X type products
    SUBSET = "subset"

    # Archived categories kept for back-compatibility
    CASE = "case"
    COMMANDER_DECK = "commander_deck"
    LAND_STATION = "land_station"
    TWO_PLAYER_STARTER_SET = "two_player_starter_set"
    DRAFT_SET = "draft_set"
    PRERELEASE_PACK = "prerelease_pack"
    PRERELEASE_CASE = "prerelease_case"

    def to_json(self) -> str | None:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        if self.value:
            return str(self.value)
        return None


class MtgjsonSealedProductSubtype(enum.Enum):
    """
    MTGJSON Sealed Product Subtype
    """

    UNKNOWN = None
    # Typically for booster_box and booster_pack
    # These should match the booster values in Booster
    DEFAULT = "default"
    DRAFT = "draft"
    PLAY = "play"
    SET = "set"
    COLLECTOR = "collector"
    JUMPSTART = "jumpstart"
    PROMOTIONAL = "promotional"
    THEME = "theme"
    WELCOME = "welcome"
    TOPPER = "topper"
    SIX = "six-card"

    # Typically for deck
    # Theme decks also use the subtype THEME
    PLANESWALKER = "planeswalker"
    CHALLENGE = "challenge"
    CHALLENGER = "challenger"
    EVENT = "event"
    CHAMPIONSHIP = "championship"
    INTRO = "intro"
    COMMANDER = "commander"
    BRAWL = "brawl"
    ARCHENEMY = "archenemy"
    PLANECHASE = "planechase"
    # Multi-deck types
    TWO_PLAYER_STARTER = "two_player_starter"
    DUEL = "duel"
    CLASH = "clash"
    BATTLE = "battle_pack"
    GAME_NIGHT = "game_night"

    # Box Set
    FROM_THE_VAULT = "from_the_vault"
    SPELLBOOK = "spellbook"
    SECRET_LAIR = "secret_lair"
    SECRET_LAIR_BUNDLE = "secret_lair_bundle"
    COMMANDER_COLLECTION = "commander_collection"
    COLLECTORS_EDITION = "collectors_edition"
    CONVENTION = "convention_exclusive"

    # Kits
    GUILD_KIT = "guild_kit"
    DECK_BUILDERS_TOOLKIT = "deck_builders_toolkit"
    LAND_STATION = "land_station"

    # Bundles
    GIFT_BUNDLE = "gift_bundle"
    FAT_PACK = "fat_pack"

    # Limited Play Aids
    DRAFT_SET = "draft_set"
    SEALED_SET = "sealed_set"
    TOURNAMENT = "tournament_deck"
    STARTER = "starter_deck"
    PRERELEASE = "prerelease_kit"

    # Anything else
    MINIMAL = "minimal_packaging"
    PREMIUM = "premium"
    ADVANCED = "advanced"
    OTHER = "other"

    def to_json(self) -> str | None:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        if self.value:
            return str(self.value)
        return None


class MtgjsonSealedProductCardObject(MTGJsonModel):
    """
    Describes a card contained within a sealed product.
    """

    foil: bool | None = Field(default=None, description="Whether the card is foil.")
    name: str = Field(description="The name of the card.")
    number: str = Field(description="The card number.")
    set: str = Field(description="The set code the card belongs to.")
    uuid: str = Field(description="The unique identifier of the card.")


class MtgjsonSealedProductDeckObject(MTGJsonModel):
    """
    Describes a deck contained within a sealed product.
    """

    name: str = Field(description="The name of the deck.")
    set: str = Field(description="The set code of the deck.")


class MtgjsonSealedProductOtherObject(MTGJsonModel):
    """
    Describes other items (like dice, rules inserts) contained in a sealed product.
    """

    name: str = Field(description="The name of the item.")


class MtgjsonSealedProductPackObject(MTGJsonModel):
    """
    Describes a booster pack contained within a sealed product.
    """

    code: str = Field(description="The set code of the pack.")
    set: str = Field(description="The set code the pack belongs to.")


class MtgjsonSealedProductSealedObject(MTGJsonModel):
    """
    Describes a nested sealed product contained within a sealed product.
    """

    count: int = Field(description="The quantity of this product.")
    name: str = Field(description="The name of the nested product.")
    set: str = Field(description="The set code.")
    uuid: str = Field(description="The UUID of the nested product.")


class MtgjsonSealedProductContentsObject(MTGJsonModel):
    """
    The contents of a sealed product.
    """

    card: list[MtgjsonSealedProductCardObject] = Field(
        default_factory=list, description="List of specific cards in the product."
    )
    deck: list[MtgjsonSealedProductDeckObject] = Field(
        default_factory=list, description="List of decks in the product."
    )
    other: list[MtgjsonSealedProductOtherObject] = Field(
        default_factory=list, description="List of other items in the product."
    )
    pack: list[MtgjsonSealedProductPackObject] = Field(
        default_factory=list, description="List of packs in the product."
    )
    sealed: list[MtgjsonSealedProductSealedObject] = Field(
        default_factory=list, description="List of other sealed products inside."
    )
    # Recursive structure definition
    variable: list[dict[str, list["MtgjsonSealedProductContentsObject"]]] = Field(
        default_factory=list, description="Variable configurations of contents."
    )


class MtgjsonSealedProductObject(MTGJsonModel):
    """
    The Sealed Product Data Model describes the properties for the purchaseable product of a Set.
    """

    card_count: int | None = Field(
        default=None, description="The number of cards in this product."
    )
    category: str | None = Field(
        default=None, description="The category of this product."
    )
    contents: MtgjsonSealedProductContentsObject | None = Field(
        default=None, description="The contents of this product."
    )
    identifiers: MtgjsonIdentifiersObject = Field(
        default_factory=MtgjsonIdentifiersObject,
        description="The identifiers associated to a product.",
    )
    language: str | None = Field(
        default=None, description="The language of the product."
    )
    name: str = Field(default="", description="The name of the product.")
    product_size: int | None = Field(
        default=None, description="The size of the product."
    )
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject,
        description="Links that navigate to websites where the product can be purchased.",
    )
    raw_purchase_urls: dict[str, str] = Field(default_factory=dict, exclude=True)
    release_date: str | None = Field(
        default=None, description="The release date in ISO 8601 format for the product."
    )
    subtype: str | None = Field(
        default=None, description="The category subtype of this product."
    )
    uuid: str = Field(
        default="",
        description="The universal unique identifier (v5) generated by MTGJSON.",
    )

    @model_validator(mode="after")
    def generate_uuid(self) -> "MtgjsonSealedProductObject":
        """
        Generate UUIDv5 for sealed product after model creation.
        Uses product name as the UUID source.
        """
        # Skip if UUID already set
        if self.uuid:
            return self

        # Generate UUID from product name
        if self.name:
            self.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, self.name))

        return self

    def build_keys_to_skip(self) -> set[str]:
        """
        Keys to exclude from JSON output
        :return: Set of keys to skip
        """
        return {"raw_purchase_urls"}

    def to_json(self) -> dict[str, Any]:
        """
        Custom JSON serialization that filters out empty values
        :return: JSON object
        """
        parent = super().to_json()
        return {key: value for key, value in parent.items() if value}
