"""MTGJSON Card Object model for individual MTG card data."""

import json
import unicodedata
from typing import TYPE_CHECKING, Any
import uuid

from pydantic import (
    Field,
    PrivateAttr,
    computed_field,
    field_validator,
    model_validator,
)

from ... import constants
from ..mtgjson_base import MTGJsonCardModel
from .mtgjson_foreign_data import MtgjsonForeignDataObject
from .mtgjson_game_formats import MtgjsonGameFormatsObject
from .mtgjson_leadership_skills import MtgjsonLeadershipSkillsObject
from .mtgjson_legalities import MtgjsonLegalitiesObject
from .mtgjson_prices import MtgjsonPricesObject
from .mtgjson_purchase_urls import MtgjsonPurchaseUrlsObject
from .mtgjson_related_cards import MtgjsonRelatedCardsObject
from .mtgjson_rulings import MtgjsonRulingObject

if TYPE_CHECKING:
    from mtgjson5.providers.uuid_cache import UuidCacheProvider


class MtgjsonCardObject(MTGJsonCardModel):
    """
    Card (Set) Data Model. Represents a specific printing of a card within a Set.
    """

    artist: str | None = Field(default=None, alias="artist", description="The artist of the card.")
    artist_ids: list[str] | None = Field(
        default=None, alias="artist_ids", description="The identifiers of the artist."
    )
    availability: MtgjsonGameFormatsObject = Field(
        default_factory=MtgjsonGameFormatsObject,
        description="Where the card is available (paper, mtgo, arena).",
    )
    booster_types: list[str] = Field(
        default_factory=list, description="Types of boosters this card appears in."
    )
    border_color: str = Field(
        default="", alias="border_color", description="The color of the card border."
    )
    card_parts: list[str] = Field(
        default_factory=list, description="UUIDs of other parts of the card."
    )
    color_identity: list[str] = Field(
        default_factory=list,
        alias="color_identity",
        description="The color identity of the card.",
    )
    colors: list[str] = Field(
        default_factory=list, alias="colors", description="The colors of the card."
    )
    count: int = Field(
        default=1, description="The number of copies of this card in the deck."
    )
    finishes: list[str] = Field(
        default_factory=list,
        alias="finishes",
        description="The finishes available for this card.",
    )
    foreign_data: list[MtgjsonForeignDataObject] = Field(
        default_factory=list, description="Data for the card in other languages."
    )
    frame_effects: list[str] = Field(
        default_factory=list,
        alias="frame_effects",
        description="The frame effects on the card.",
    )
    frame_version: str = Field(
        default="", alias="frame", description="The version of the card frame."
    )
    has_foil: bool = Field(
        default=False, description="If the card is available in foil."
    )
    has_non_foil: bool = Field(
        default=False, description="If the card is available in non-foil."
    )
    keywords: list[str] = Field(
        default_factory=list, alias="keywords", description="Keywords on the card."
    )
    language: str = Field(
        default="", alias="lang", description="The language of the card."
    )
    layout: str = Field(
        default="", alias="layout", description="The layout of the card."
    )
    legalities: MtgjsonLegalitiesObject = Field(
        default_factory=MtgjsonLegalitiesObject,
        alias="legalities",
        description="Legalities.",
    )
    mana_cost: str | None = Field(
        default="", alias="mana_cost", description="Mana cost."
    )
    mana_value: float = Field(default=0.0, alias="cmc", description="Mana value.")
    name: str = Field(default="", alias="name", description="Card name.")
    number: str = Field(
        default="0", alias="collector_number", description="Collector number."
    )
    original_printings: list[str] = Field(
        default_factory=list, description="Original printing set codes."
    )
    other_face_ids: list[str] = Field(
        default_factory=list, description="UUIDs of other faces."
    )
    power: str | None = Field(default="", alias="power", description="Power.")
    printings: list[str] = Field(
        default_factory=list, description="Set codes of printings."
    )
    promo_types: list[str] = Field(
        default_factory=list, alias="promo_types", description="Types of promo."
    )
    purchase_urls: MtgjsonPurchaseUrlsObject = Field(
        default_factory=MtgjsonPurchaseUrlsObject, description="Purchase URLs."
    )
    rarity: str = Field(default="", alias="rarity", description="Rarity.")
    rebalanced_printings: list[str] = Field(
        default_factory=list, description="Sets where this was rebalanced."
    )
    subtypes: list[str] = Field(default_factory=list, description="Subtypes.")
    supertypes: list[str] = Field(default_factory=list, description="Supertypes.")
    text: str | None = Field(default="", alias="oracle_text", description="Rules text.")
    toughness: str | None = Field(
        default="", alias="toughness", description="Toughness."
    )
    type: str = Field(default="", alias="type_line", description="Full type line.")
    types: list[str] = Field(default_factory=list, description="Types.")
    uuid: str = Field(default="", description="Unique identifier.")
    variations: list[str] = Field(
        default_factory=list, description="UUIDs of variations."
    )
    face_mana_value: float | None = Field(
        default=0.0, description="The mana value of the face."
    )
    prices: MtgjsonPricesObject = Field(default_factory=MtgjsonPricesObject)

    set_code: str = Field(default="", alias="set", description="Set code.")
    is_token: bool = Field(default=False, exclude=True)
    raw_purchase_urls: dict[str, str] = Field(default_factory=dict, exclude=True)

    attraction_lights: list[int] | None = Field(
        default=None,
        alias="attraction_lights",
        description="The attraction lights lit on the card.",
    )
    color_indicator: list[str] | None = Field(
        default=None,
        alias="color_indicator",
        description="The color indicator of the card.",
    )
    defense: str | None = Field(
        default=None, alias="defense", description="The defense of the card."
    )
    duel_deck: str | None = Field(
        default=None, description="The duel deck code if applicable."
    )
    edhrec_rank: int | None = Field(
        default=None, alias="edhrec_rank", description="The EDHREC rank."
    )
    edhrec_saltiness: float | None = Field(
        default=None, description="The EDHREC saltiness score."
    )
    face_flavor_name: str | None = Field(
        default=None, description="The flavor name on the face."
    )
    face_name: str | None = Field(default=None, description="The name on the face.")
    first_printing: str | None = Field(
        default=None, description="The set code of the first printing of the card."
    )
    flavor_name: str | None = Field(
        default=None, alias="flavor_name", description="The flavor name of the card."
    )
    flavor_text: str | None = Field(
        default=None, alias="flavor_text", description="The flavor text of the card."
    )
    hand: str | None = Field(
        default=None, alias="hand_modifier", description="The hand modifier."
    )
    has_alternative_deck_limit: bool | None = Field(
        default=None, description="If the card allows more than 4 copies."
    )
    has_content_warning: bool | None = Field(
        default=None,
        alias="content_warning",
        description="If the card has a content warning.",
    )
    is_alternative: bool | None = Field(
        default=None, description="If this is an alternative printing."
    )
    is_foil: bool | None = Field(
        default=None, alias="foil", description="If this specific card entry is foil."
    )
    is_full_art: bool | None = Field(
        default=None, alias="full_art", description="If the card is full art."
    )
    is_funny: bool | None = Field(
        default=None, description="If the card is from a funny set."
    )
    is_game_changer: bool | None = Field(
        default=None,
        alias="game_changer",
        description="If the card changes the rules of the game.",
    )
    is_online_only: bool | None = Field(
        default=None,
        alias="digital",
        description="If the card is only available online.",
    )
    is_oversized: bool | None = Field(
        default=None, alias="oversized", description="If the card is oversized."
    )
    is_promo: bool | None = Field(
        default=None, alias="promo", description="If the card is a promo."
    )
    is_rebalanced: bool | None = Field(
        default=None, description="If the card has been rebalanced."
    )
    is_reprint: bool | None = Field(
        default=None, alias="reprint", description="If the card is a reprint."
    )
    is_reserved: bool | None = Field(
        default=None,
        alias="reserved",
        description="If the card is on the Reserved List.",
    )
    is_starter: bool | None = Field(
        default=None, description="If the card is from a starter product."
    )
    is_story_spotlight: bool | None = Field(
        default=None,
        alias="story_spotlight",
        description="If the card is a story spotlight.",
    )
    is_textless: bool | None = Field(
        default=None, alias="textless", description="If the card is textless."
    )
    is_timeshifted: bool | None = Field(
        default=None, description="If the card is timeshifted."
    )
    leadership_skills: MtgjsonLeadershipSkillsObject | None = Field(
        default=None, description="Leadership skills."
    )
    life: str | None = Field(
        default=None, alias="life_modifier", description="Life modifier."
    )
    loyalty: str | None = Field(default=None, alias="loyalty", description="Loyalty.")
    orientation: str | None = Field(default=None, description="Orientation.")
    original_release_date: str | None = Field(
        default=None, alias="released_at", description="Original release date."
    )
    original_text: str | None = Field(default=None, description="Original rules text.")
    original_type: str | None = Field(default=None, description="Original type line.")
    related_cards: MtgjsonRelatedCardsObject | None = Field(
        default=None, description="Related cards."
    )
    reverse_related: list[str] | None = Field(
        default=None,
        description="A list of card names associated to a card, such as 'meld' cards and token creation.",
    )
    rulings: list[MtgjsonRulingObject] | None = Field(
        default=None, description="Rulings."
    )
    security_stamp: str | None = Field(
        default=None, alias="security_stamp", description="Security stamp type."
    )
    side: str | None = Field(default=None, description="Side (a/b).")
    signature: str | None = Field(default=None, description="Signature on the card.")
    source_products: dict[str, list[str]] | None = Field(
        default=None, description="Source products."
    )
    subsets: list[str] | None = Field(default=None, description="Subsets.")
    watermark: str | None = Field(
        default=None, alias="watermark", description="Watermark."
    )
    printed_name: str | None = Field(
        default=None, alias="printed_name", description="The printed name of the card."
    )
    printed_type: str | None = Field(
        default=None,
        alias="printed_type_line",
        description="The printed type line of the card.",
    )
    printed_text: str | None = Field(
        default=None, alias="printed_text", description="The printed text of the card."
    )
    face_printed_name: str | None = Field(
        default=None, description="The printed name on the face of the card."
    )
    is_etched: bool | None = Field(default=None, description="If the card is etched.")

    _names: list[str] | None = PrivateAttr(default=None)
    _illustration_ids: list[str] = PrivateAttr(default_factory=list)
    _watermark_resource: dict[str, list[Any]] = PrivateAttr(default_factory=dict)

    # model validators with mode set to 'after' will run after initialization automatically
    @model_validator(mode="after")
    def generate_uuid(self) -> "MtgjsonCardObject":
        """
        Generate UUIDv5 for card after model creation.
        Also generates UUIDv4 for legacy support and foreign data UUIDs.
        """
        # Skip if UUID already set
        if self.uuid:
            return self

        # Check for required fields
        if not hasattr(self, "identifiers"):
            return self

        # Generate v4 ID for legacy support
        self.identifiers.mtgjson_v4_id = self._generate_v4_uuid()

        # Get scryfall_id and side for v5 UUID generation
        scryfall_id = getattr(self.identifiers, "scryfall_id", None)
        if not scryfall_id:
            return self

        side = getattr(self, "side", None) or "a"
        id_source_v5 = str(scryfall_id) + side

        # Generate foreign data UUIDs if foreign_data exists
        self._add_foreign_uuids(id_source_v5)

        # Check UUID cache for previously generated UUIDs
        try:
            from mtgjson5.providers.uuid_cache import UuidCacheProvider

            cached_uuid = UuidCacheProvider().get_uuid(str(scryfall_id), side)
            if cached_uuid:
                self.uuid = cached_uuid
                return self
        except ImportError:
            pass

        # Generate new v5 UUID
        self.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v5))

        return self

    def _generate_v4_uuid(self) -> str:
        """
        Generate MTGJSONv4 UUID for legacy support.
        Implementation based on card type (token vs normal card).
        """
        types = getattr(self, "types", None) or []

        if {"Token", "Card"}.intersection(types):
            # Tokens have a special generation method
            face_name = getattr(self, "face_name", None)
            name = getattr(self, "name", None)
            colors = getattr(self, "colors", None) or []
            power = getattr(self, "power", None) or ""
            toughness = getattr(self, "toughness", None) or ""
            side = getattr(self, "side", None) or ""
            set_code = getattr(self, "set_code", "")
            scryfall_id = getattr(self.identifiers, "scryfall_id", None) or ""

            id_source_v4 = (
                (face_name if face_name else name or "")
                + "".join(colors)
                + power
                + toughness
                + side
                + set_code[1:].upper()
                + str(scryfall_id)
            )
        else:
            # Normal cards only need a few pieces of data
            name = getattr(self, "name", None) or ""
            set_code = getattr(self, "set_code", "")
            scryfall_id = getattr(self.identifiers, "scryfall_id", None) or ""

            id_source_v4 = name + set_code + str(scryfall_id)

        return str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v4))

    def _add_foreign_uuids(self, id_source_prefix: str) -> None:
        """
        Add unique identifiers to each language's printing of a card.

        :param id_source_prefix: Base UUID source (scryfall_id + side)
        """
        foreign_data = getattr(self, "foreign_data", None)
        if not foreign_data:
            return

        for language_entry in foreign_data:
            language = getattr(language_entry, "language", None)
            if language:
                language_entry.uuid = str(
                    uuid.uuid5(uuid.NAMESPACE_DNS, f"{id_source_prefix}_{language}")
                )

    # Model Validator for Scryfall Data Transformation
    @model_validator(mode="before")
    @classmethod
    def extract_identifiers_from_scryfall(cls, data: Any) -> Any:
        """
        Extract identifier fields from top-level Scryfall data and nest them under 'identifiers'.

        This allows Pydantic's field aliases in MtgjsonIdentifiersObject to work correctly.
        """
        if not isinstance(data, dict):
            return data

        # Skip if identifiers is already provided (already transformed)
        if "identifiers" in data and isinstance(data["identifiers"], dict):
            return data

        # List of identifier field names that should be moved to the identifiers sub-object
        # Using both Scryfall names (aliases) and MTGJSON names
        identifier_fields = {
            "id",
            "oracle_id",
            "illustration_id",
            "card_back_id",  # Scryfall names
            "arena_id",
            "cardmarket_id",
            "mtgo_id",
            "mtgo_foil_id",  # Scryfall names
            "multiverse_ids",
            "tcgplayer_id",
            "tcgplayer_etched_id",  # Scryfall names
            "scryfall_id",
            "scryfall_oracle_id",
            "scryfall_illustration_id",  # MTGJSON names
            "scryfall_card_back_id",
            "mcm_id",
            "mtg_arena_id",  # MTGJSON names
            "tcgplayer_product_id",
            "tcgplayer_etched_product_id",
            "multiverse_id",  # MTGJSON names
        }

        # Extract identifier fields from top-level data
        identifiers_data = {}
        for field in identifier_fields:
            if field in data:
                identifiers_data[field] = data[field]

        # Handle oracle_id fallback from card_faces if not present at top level
        if "oracle_id" not in identifiers_data and "card_faces" in data:
            face_id = data.get("_face_id", 0)
            if (
                isinstance(data["card_faces"], list)
                and len(data["card_faces"]) > face_id
            ):
                face_oracle_id = data["card_faces"][face_id].get("oracle_id")
                if face_oracle_id:
                    identifiers_data["oracle_id"] = face_oracle_id

        # Handle illustration_id fallback from card_faces if not present at top level
        if "illustration_id" not in identifiers_data and "card_faces" in data:
            face_id = data.get("_face_id", 0)
            if (
                isinstance(data["card_faces"], list)
                and len(data["card_faces"]) > face_id
            ):
                face_illustration_id = data["card_faces"][face_id].get(
                    "illustration_id"
                )
                if face_illustration_id:
                    identifiers_data["illustration_id"] = face_illustration_id

        # Only create identifiers if we found any identifier fields
        if identifiers_data:
            # Create or merge with existing identifiers
            if "identifiers" not in data:
                data["identifiers"] = {}
            data["identifiers"].update(identifiers_data)

        return data

    @model_validator(mode="before")
    @classmethod
    def handle_multi_face_cards(cls, data: Any) -> Any:
        """
        Handle multi-face cards by merging face-specific data into the main card data.

        For cards with card_faces, extracts the relevant face data based on _face_id
        and merges it with the top-level data, handling fallbacks appropriately.
        """
        if not isinstance(data, dict):
            return data

        # Skip if no card_faces present
        if "card_faces" not in data or not isinstance(data["card_faces"], list):
            return data

        face_id = data.get("_face_id", 0)
        card_faces = data["card_faces"]

        # Validate face_id
        if face_id >= len(card_faces):
            return data

        face_data = card_faces[face_id]

        # Split and store names if this is face 0
        if face_id == 0 and "name" in data:
            name_parts = [part.strip() for part in data["name"].split("//")]
            data["_names_list"] = name_parts

        # Merge face-specific fields (face data takes precedence over top-level)
        # These fields should come from the specific face if available
        face_priority_fields = [
            "mana_cost",
            "type_line",
            "oracle_text",
            "power",
            "toughness",
            "colors",
            "color_indicator",
            "loyalty",
            "defense",
            "artist",
            "artist_ids",
            "flavor_text",
            "watermark",
        ]

        for field in face_priority_fields:
            if field in face_data:
                # For fields that exist in face, use face data
                data[field] = face_data[field]

        # Handle face_name - use the name from the specific face
        if "name" in face_data:
            data["face_name"] = face_data["name"]

        # Handle printed versions
        if "printed_name" in face_data:
            data["face_printed_name"] = face_data["printed_name"]
        if "printed_text" in face_data:
            data["face_printed_text"] = face_data["printed_text"]
        if "printed_type_line" in face_data:
            data["face_printed_type_line"] = face_data["printed_type_line"]

        # Handle mana value for this face
        if "cmc" in face_data:
            data["face_mana_value"] = face_data["cmc"]

        # Handle flavor name
        if "flavor_name" in face_data:
            data["face_flavor_name"] = face_data["flavor_name"]

        return data

    # Field Validators for Transformations
    @field_validator("set_code", mode="before")
    @classmethod
    def uppercase_set_code(cls, v: Any) -> str:
        """Ensure set code is uppercase"""
        return v.upper() if isinstance(v, str) else v

    @field_validator("language", mode="before")
    @classmethod
    def map_language(cls, v: Any) -> str:
        """Map language code to full language name"""
        if not v:
            return "unknown"
        return constants.LANGUAGE_MAP.get(v, "unknown")

    @field_validator("frame_effects", mode="before")
    @classmethod
    def sort_frame_effects(cls, v: Any) -> list[str]:
        """Sort frame effects alphabetically"""
        return sorted(v) if v else []

    @field_validator("color_identity", mode="before")
    @classmethod
    def ensure_color_identity_is_list(cls, v: Any) -> list[str]:
        """Ensure color_identity is a list (sometimes empty string from Scryfall)"""
        return v if isinstance(v, list) else []

    @model_validator(mode="after")
    def parse_type_line(self) -> "MtgjsonCardObject":
        """
        Parse type_line to extract supertypes, types, and subtypes.

        Handles special cases like multi-word subtypes (e.g., 'Time Lord').
        """
        if not self.type:
            return self

        type_line = self.type
        supertypes = []
        types = []
        subtypes = []

        # Split by em-dash or hyphen to separate main types from subtypes
        if " — " in type_line:
            main_part, sub_part = type_line.split(" — ", 1)
        elif " - " in type_line:
            main_part, sub_part = type_line.split(" - ", 1)
        else:
            main_part = type_line
            sub_part = ""

        # Parse main part for supertypes and types
        main_words = main_part.split()
        for word in main_words:
            if word in constants.SUPER_TYPES:
                supertypes.append(word)
            else:
                types.append(word)

        # Parse subtypes (handle multi-word subtypes)
        if sub_part:
            remaining = sub_part
            for multi_word_type in constants.MULTI_WORD_SUB_TYPES:
                if multi_word_type in remaining:
                    subtypes.append(multi_word_type)
                    remaining = remaining.replace(multi_word_type, "")

            # Add remaining single-word subtypes
            for word in remaining.split():
                if word:
                    subtypes.append(word)

        self.supertypes = supertypes
        self.types = types
        self.subtypes = subtypes

        return self

    @model_validator(mode="after")
    def compute_availability(self) -> "MtgjsonCardObject":
        """
        Compute availability flags from Scryfall's 'games' field and identifiers.

        The 'games' field is a temporary field passed from Scryfall data.
        """
        # Games field comes from Scryfall data
        games = getattr(self, "_games_temp", [])

        if not hasattr(self, "availability") or not self.availability:
            self.availability = MtgjsonGameFormatsObject()

        # Arena availability
        self.availability.arena = "arena" in games or (
            self.identifiers.mtg_arena_id is not None
        )

        # MTGO availability
        self.availability.mtgo = "mtgo" in games or (
            self.identifiers.mtgo_id is not None
        )

        # Paper availability
        self.availability.paper = not self.is_online_only

        # Shandalar availability
        self.availability.shandalar = "astral" in games

        # Dreamcast availability
        self.availability.dreamcast = "sega" in games

        return self

    @model_validator(mode="after")
    def compute_is_funny(self) -> "MtgjsonCardObject":
        """
        Determine if card is from a funny/un-set.

        Based on set_type being 'funny' and frame being 'future'.
        """
        # set_type_temp is passed from Scryfall data
        set_type = getattr(self, "_set_type_temp", "")

        self.is_funny = set_type in {"funny"} and self.frame_version == "future"

        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ascii_name(self) -> str:
        """Generate ASCII-safe name from unicode name"""
        if not self.name:
            return ""
        # Normalize unicode and encode to ASCII
        normalized = unicodedata.normalize("NFD", self.name)
        ascii_bytes = normalized.encode("ascii", "ignore")
        return ascii_bytes.decode("ascii")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def converted_mana_cost(self) -> float:
        """Deprecated alias for mana_value (backward compatibility)"""
        return self.mana_value

    def __eq__(self, other: Any) -> bool:
        """
        Determine if two MTGJSON Card Objects are equal
        First comparison: Card Number
        Second comparison: Side Letter
        :param other: Other card
        :return: Same object or not
        """
        if not isinstance(other, MtgjsonCardObject):
            return False
        return bool(
            self.number == other.number and (self.side or "") == (other.side or "")
        )

    def __lt__(self, other: Any) -> bool:
        """
        Less than operation
        First comparison: Card Number
        Second comparison: Side Letter
        :param other: Other card
        :return: Less than or not
        """
        if not isinstance(other, MtgjsonCardObject):
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
            if not self_side and not other_side:
                return bool(self.number < other.number)
            return self_side < other_side

        if self_number_clean_int == other_number_clean_int:
            if len(self_number_clean) != len(other_number_clean):
                return len(self_number_clean) < len(other_number_clean)
            return self_side < other_side

        return self_number_clean_int < other_number_clean_int

    def set_illustration_ids(self, illustration_ids: list[str]) -> None:
        """
        Set internal illustration IDs for this card to
        better identify what side we're working on,
        especially for Art and Token cards
        :param illustration_ids: Illustration IDs of the card faces
        """
        self._illustration_ids = illustration_ids

    def get_illustration_ids(self) -> list[str]:
        """
        Get the internal illustration IDs roster for this card
        to better identify the sides for Art and Token cards
        :return: Illustration IDs
        """
        return self._illustration_ids

    def get_names(self) -> list[str]:
        """
        Get internal names array for this card
        :return: Names array or empty list
        """
        return self._names or []

    def set_names(self, names: list[str] | None) -> None:
        """
        Set internal names array for this card
        :param names: Names list (optional)
        """
        self._names = list(map(str.strip, names)) if names else None

    def append_names(self, name: str) -> None:
        """
        Append to internal names array for this card
        :param name: Name to append
        """
        if self._names:
            self._names.append(name)
        else:
            self.set_names([name])

    def set_watermark(self, watermark: str | None) -> None:
        """
        Watermarks sometimes aren't specific enough, so we
        must manually update them. This only applies if the
        watermark is "set" and then we will append the actual
        set code to the watermark.
        :param watermark: Current watermark
        """
        if not watermark:
            return

        if not self._watermark_resource:
            with constants.RESOURCE_PATH.joinpath("set_code_watermarks.json").open(
                encoding="utf-8"
            ) as f:
                self._watermark_resource = json.load(f)

        if watermark == "set":
            for card in self._watermark_resource.get(self.set_code.upper(), []):
                if self.name in card["name"].split(" // "):
                    watermark = str(card["watermark"])
                    break

        self.watermark = watermark

    def get_atomic_keys(self) -> list[str]:
        """
        Get attributes of a card that don't change
        from printing to printing
        :return: Keys that are atomic
        """
        return self._atomic_keys

    def build_keys_to_skip(self) -> set[str]:
        """
        Build this object's instance of what keys to skip under certain circumstances
        :return: What keys to skip over
        """
        if self.is_token:
            excluded_keys = self._exclude_for_tokens.copy()
        else:
            excluded_keys = self._exclude_for_cards.copy()

        excluded_keys = excluded_keys.union({"is_token", "raw_purchase_urls"})

        for key, value in self.__dict__.items():
            if not value:
                if key not in self._allow_if_falsey:
                    excluded_keys.add(key)

        return excluded_keys
