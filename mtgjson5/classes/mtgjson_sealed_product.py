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
    UNKNOWN = None

    # Packs!
    BOOSTER_PACK = "booster_pack"

    # A Box containing boosters
    BOOSTER_BOX = "booster_box"

    # A box containing a variable amount of booster boxes
    BOOSTER_CASE = "booster_case"

    # Decks!
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

    def to_json(self) -> Optional[str]:
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

    def to_json(self) -> Optional[str]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        if self.value:
            return str(self.value)
        return None


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
    language: Optional[str]
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
