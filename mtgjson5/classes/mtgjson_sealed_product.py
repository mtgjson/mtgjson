"""
MTGJSON Singular Sealed Product Object
"""
import enum
from typing import Any, Dict, Optional

from ..utils import to_camel_case
from .mtgjson_identifiers import MtgjsonIdentifiersObject
from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject


class MtgjsonSealedProductCategory(enum.Enum):
    """
    MTGJSON Sealed Product Category
    """

    # ¯\_(ツ)_/¯
    UNKNOWN = ""

    # A box containing a variable amount of sealed product
    CASE = "case"

    # A Box containing boosters
    BOOSTER_BOX = "booster_box"

    # Packs!
    BOOSTER_PACK = "booster_pack"

    # A box containing a known list of cards
    BOX_SET = "box_set"

    # A prerelease pack
    PRERELEASE_PACK = "prerelease_pack"

    # A commander deck
    COMMANDER_DECK = "commander_deck"

    # A Box containing decks
    DECK_BOX = "deck_box"

    # Decks!
    DECK = "deck"

    # A boxed group of boosters, typically with a dice
    BUNDLE = "bundle"

    # Lands!
    LAND_STATION = "land_station"

    # The minimum number of boosters for a single player draft
    DRAFT_SET = "draft_set"

    # A set of two decks, typically for starters
    TWO_PLAYER_STARTER_SET = "two_player_starter_set"

    # A group of packs or decks
    SUBSET = "subset"

    def to_json(self) -> str:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.value


class MtgjsonSealedProductSubtype(enum.Enum):
    """
    MTGJSON Sealed Product Subtype
    """

    UNKNOWN = ""

    # Typically for booster_box and booster_pack
    # These should match the booster values in Booster
    DEFAULT = "default"
    SET = "set"
    COLLECTOR = "collector"
    JUMPSTART = "jumpstart"

    # Typically for deck
    THEME = "theme"
    STARTER = "starter"
    PLANESWALKER = "planeswalker"
    CHALLENGE = "challenge"
    EVENT = "event"
    CHAMPIONSHIP = "championship"
    INTRO = "intro"

    # Box Set
    ARCHENEMY = "archenemy"
    PLANECHASE = "planechase"
    FROM_THE_VAULT = "from_the_vault"
    SPELLBOOK = "spellbook"
    SECRET_LAIR = "secret_lair"
    DECK_BUILDERS_TOOLKIT = "deck_builders_toolkit"

    # Anything else
    MINIMAL = "minimal_packaging"
    TOPPER = "topper"
    PREMIUM = "premium"
    VIP = "vip"
    DELUXE = "deluxe"
    ADVANCED = "advanced"
    CLASH = "clash"
    BATTLE = "battle_pack"

    def to_json(self) -> str:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.value


class MtgjsonSealedProductObject:
    """
    MTGJSON Singular Sealed Product Object
    """

    name: str
    uuid: str
    identifiers: MtgjsonIdentifiersObject
    purchase_urls: MtgjsonPurchaseUrlsObject
    raw_purchase_urls: Dict[str, str]
    release_date: Optional[str]
    category: Optional[MtgjsonSealedProductCategory]
    subtype: Optional[MtgjsonSealedProductSubtype]
    contents: Optional[Dict[str, Any]]  # Enumerated product contents
    product_size: Optional[int]  # Number of packs in a booster box [DEPRECATED]
    card_count: Optional[int]  # Number of cards in a booster pack or deck
    __skip_keys = [
        "raw_purchase_urls",
    ]

    def __init__(self) -> None:
        self.identifiers = MtgjsonIdentifiersObject()
        self.purchase_urls = MtgjsonPurchaseUrlsObject()
        self.raw_purchase_urls = {}

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key
            and not callable(value)
            and key not in self.__skip_keys
            and value
        }
