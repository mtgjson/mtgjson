"""
MTGJSON Set Builder
"""

import json
import logging
import pathlib
import re
import unicodedata
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import polars as pl

from . import constants
from .cache_builder import GLOBAL_CACHE
from .classes import (
    MtgjsonCardObject,
    MtgjsonForeignDataObject,
    MtgjsonGameFormatsObject,
    MtgjsonLeadershipSkillsObject,
    MtgjsonLegalitiesObject,
    MtgjsonMetaObject,
    MtgjsonRelatedCardsObject,
    MtgjsonRulingObject,
    MtgjsonSealedProductObject,
    MtgjsonSetObject,
    MtgjsonTranslationsObject,
)
from .constants import RESOURCE_PATH
from .parallel_call import parallel_call
from .providers import (
    CardKingdomProvider,
    CardMarketProvider,
    EdhrecProviderCardRanks,
    GathererProvider,
    GitHubBoostersProvider,
    GitHubCardSealedProductsProvider,
    GitHubDecksProvider,
    GitHubSealedProvider,
    MtgWikiProviderSecretLair,
    MultiverseBridgeProvider,
    ScryfallProvider,
    ScryfallProviderOrientationDetector,
    ScryfallProviderSetLanguageDetector,
    TCGPlayerProvider,
    UuidCacheProvider,
    WhatsInStandardProvider,
)
from .utils import get_str_or_none, load_local_set_data, url_keygen

LOGGER = logging.getLogger(__name__)


@dataclass
class CardBuildContext:
    """Context for building a single card."""

    scryfall: dict[str, Any]
    face_id: int = 0
    is_token: bool = False
    set_release_date: str = ""

    @property
    def is_multi_face(self) -> bool:
        """Check if this card has multiple faces."""
        return bool(self.scryfall.get("card_faces"))

    @property
    def face_data(self) -> dict[str, Any]:
        """Get the relevant face data, or the card itself for single-face."""
        if self.is_multi_face:
            faces = self.scryfall.get("card_faces") or []
            if self.face_id < len(faces):
                return cast(dict[str, Any], faces[self.face_id])
            # fallback to first face or card itself
            return cast(dict[str, Any], faces[0] if faces else self.scryfall)
        return self.scryfall

    @property
    def face_count(self) -> int:
        """Get the number of faces on this card."""
        if self.is_multi_face:
            return len(self.scryfall["card_faces"])
        return 1

    @property
    def side(self) -> str:
        """Get the side identifier for this face."""
        if self.is_multi_face:
            return chr(ord("a") + self.face_id)
        return ""


def parse_foreign(
    set_code: str, card_number: str, _card_name: str
) -> List[MtgjsonForeignDataObject]:
    """
    Get the foreign printings information for a specific card from cache
    :param set_code: Set code
    :param card_number: Card's collector number
    :param _card_name: Card name (reserved for future use)
    :return: Foreign entries object
    """
    foreign_entries = GLOBAL_CACHE.get_foreign_data(set_code, card_number)
    if not foreign_entries:
        LOGGER.debug(f"No foreign data found for {set_code} #{card_number}")
        return []

    card_foreign_entries: List[MtgjsonForeignDataObject] = []

    for entry in foreign_entries:
        card_foreign_entry = MtgjsonForeignDataObject()

        card_foreign_entry.language = entry.get("language", "")
        card_foreign_entry.identifiers.scryfall_id = entry.get("scryfall_id")

        multiverse_id = entry.get("multiverse_id")
        if multiverse_id:
            card_foreign_entry.multiverse_id = multiverse_id
            card_foreign_entry.identifiers.multiverse_id = str(multiverse_id)

        card_foreign_entry.name = entry.get("name")
        card_foreign_entry.text = entry.get("text")
        card_foreign_entry.flavor_text = entry.get("flavor_text")
        card_foreign_entry.type = entry.get("type")
        card_foreign_entry.face_name = entry.get("face_name")

        # https://github.com/mtgjson/mtgjson/issues/611
        if set_code.upper() == "IKO" and card_foreign_entry.language == "Japanese":
            if card_foreign_entry.name:
                card_foreign_entry.name = str(card_foreign_entry.name).split(
                    " //", maxsplit=1
                )[0]

        if card_foreign_entry.name:
            card_foreign_entries.append(card_foreign_entry)

    return card_foreign_entries


def parse_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Given a card type string, split it up into its raw components: super, sub, and type
    :param card_type: Card type string to parse
    :return: Tuple (super, type, sub) of the card's attributes
    """
    sub_types: List[str] = []
    super_types: List[str] = []
    types: List[str] = []

    supertypes_and_types: str
    if "—" not in card_type:
        supertypes_and_types = card_type
    else:
        split_type: List[str] = card_type.split("—")
        supertypes_and_types = split_type[0]
        subtypes: str = split_type[1]

        # Planes are an entire sub-type, whereas normal cards
        # are split by spaces... until they aren't #WHO
        if card_type.startswith("Plane"):
            sub_types = [subtypes.strip()]
        else:
            special_case_found = False
            for special_case in constants.MULTI_WORD_SUB_TYPES:
                if special_case in subtypes:
                    subtypes = subtypes.replace(
                        special_case, special_case.replace(" ", "!")
                    )
                    special_case_found = True

            sub_types = [x.strip() for x in subtypes.split() if x]
            if special_case_found:
                for i, sub_type in enumerate(sub_types):
                    sub_types[i] = sub_type.replace("!", " ")

    for value in supertypes_and_types.split():
        if value in constants.SUPER_TYPES:
            super_types.append(value)
        elif value:
            types.append(value)

    return super_types, types, sub_types


def get_card_colors(mana_cost: str) -> List[str]:
    """
    For some cards, we may have to manually determine the card's color.
    :param mana_cost: Mana cost string
    :return: Colors based on mana cost
    """
    color_options: List[str] = ["W", "U", "B", "R", "G"]

    ret_val = []
    for color in color_options:
        if color in mana_cost:
            ret_val.append(color)

    return ret_val


def get_scryfall_set_data(set_code: str) -> Optional[Dict[str, Any]]:
    """
    Get a Scryfall set header for a specific set from cache
    :param set_code: Set to grab header for
    :return: Set header, if it exists
    """
    set_data = GLOBAL_CACHE.get_set(set_code.upper())
    if set_data is None:
        LOGGER.warning(f"Set {set_code} not found in cache")
        return None
    return set_data


def is_number(string: str) -> bool:
    """See if a given string is a number (int or float)"""
    try:
        float(string)
        return True
    except ValueError:
        pass

    try:
        unicodedata.numeric(string)
        return True
    except (TypeError, ValueError):
        pass

    return False


def get_card_cmc(mana_cost: str) -> float:
    """
    For some cards, we may have to manually update the converted mana cost.
    We do this by reading the inner components of each pair of {} and
    deciphering what the contents mean. If number, we're good. Otherwise +1.
    :param mana_cost: Mana cost string
    :return: One sided cmc
    """
    total: float = 0.0

    symbol: List[str] = re.findall(r"{([^{]*)}", mana_cost.strip())
    for element in symbol:
        # Address 2/W, G/W, etc as "higher" cost always first
        if "/" in element:
            element = element.split("/")[0]

        if is_number(element):
            total += float(element)
        elif element in ["X", "Y", "Z"]:  # Placeholder mana
            continue
        elif element[0] == "H":  # Half mana
            total += 0.5
        else:
            total += 1

    return total


def parse_printings(oracle_id: Optional[str]) -> List[str]:
    """
    Given an oracle_id, extract all sets a card was printed in from cache
    :param oracle_id: Oracle ID to look up printings for
    :return: List of all sets a specific card was printed in
    """
    if not oracle_id or GLOBAL_CACHE.printings_map is None:
        return []

    result = GLOBAL_CACHE.printings_map.filter(pl.col("oracle_id") == oracle_id)
    if result.is_empty():
        LOGGER.debug(f"No printings found for oracle_id: {oracle_id}")
        return []

    printings = result["printings"][0]
    return sorted([s.upper() for s in printings])


def parse_legalities(sf_card_legalities: Dict[str, str]) -> MtgjsonLegalitiesObject:
    """
    Given a Scryfall legalities dictionary, convert it to MTGJSON format
    :param sf_card_legalities: Scryfall legalities
    :return: MTGJSON legalities
    """
    card_legalities = MtgjsonLegalitiesObject()
    for key, value in sf_card_legalities.items():
        if value != "not_legal":
            setattr(card_legalities, key.lower(), value.capitalize())

    return card_legalities


def parse_rulings(oracle_id: Optional[str]) -> List[MtgjsonRulingObject]:
    """
    Get rulings for a card from cache by oracle_id
    :param oracle_id: Oracle ID to look up rulings for
    :return: MTGJSON rulings list
    """
    if not oracle_id or GLOBAL_CACHE.rulings_map is None:
        return []

    result = GLOBAL_CACHE.rulings_map.filter(pl.col("oracle_id") == oracle_id)
    if result.is_empty():
        return []

    mtgjson_rules: List[MtgjsonRulingObject] = []
    for ruling in result["rulings"][0]:
        mtgjson_rule = MtgjsonRulingObject(ruling["published_at"], ruling["comment"])
        mtgjson_rules.append(mtgjson_rule)

    return sorted(mtgjson_rules, key=lambda ruling: (ruling.date, ruling.text))


def add_rebalanced_to_original_linkage(mtgjson_set: MtgjsonSetObject) -> None:
    """
    When Wizards rebalances a card, they break the link between
    the new card and the original card. We will create a two-way
    linkage back to and from the original card,
    should that prove useful to the end user.
    :param mtgjson_set MTGJSON Set object
    """
    LOGGER.info(f"Linking rebalanced cards for {mtgjson_set.code}")

    for card in mtgjson_set.cards:
        if getattr(card, "is_rebalanced", False):
            original_card_name_to_find = card.name.replace("A-", "")

            original_card_uuids = []
            for inner_card in mtgjson_set.cards:
                if inner_card.name == original_card_name_to_find:
                    # Doubly link these cards
                    original_card_uuids.append(inner_card.uuid)
                    if not hasattr(inner_card, "rebalanced_printings"):
                        inner_card.rebalanced_printings = []
                    inner_card.rebalanced_printings.append(card.uuid)

            card.original_printings = original_card_uuids


def relocate_miscellaneous_tokens(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Sometimes tokens find their way into the main set. This will
    remove them from the cards array and sets an internal marker
    to be dealt with later down the line
    :param mtgjson_set: MTGJSON Set object
    """
    LOGGER.info(f"Relocate tokens for {mtgjson_set.code}")
    token_types = {"token", "double_faced_token", "emblem", "art_series", "Dungeon"}

    # Identify unique tokens from cards
    tokens_found = {
        card.identifiers.scryfall_id
        for card in mtgjson_set.cards
        if (
            card.layout in token_types
            or card.type in token_types
            or "Token" in card.type
        )
        and card.identifiers.scryfall_id
    }

    # Remove tokens from cards
    mtgjson_set.cards[:] = (
        card
        for card in mtgjson_set.cards
        if (
            card.layout not in token_types
            and card.type not in token_types
            and "Token" not in card.type
        )
    )

    # Get token scryfall objects from cache
    if GLOBAL_CACHE.cards_df is not None and tokens_found:
        mtgjson_set.extra_tokens = GLOBAL_CACHE.cards_df.filter(
            pl.col("id").is_in(list(tokens_found))
        ).to_dicts()
    else:
        mtgjson_set.extra_tokens = []

    LOGGER.info(f"Finished relocating tokens for {mtgjson_set.code}")


def mark_duel_decks(set_code: str, mtgjson_cards: List[MtgjsonCardObject]) -> None:
    """
    For Duel Decks, we need to determine which "deck" the card
    can be found in. This is a convoluted, but correct, approach
    at solving that problem.
    :param set_code: Set to work on
    :param mtgjson_cards: Card Objects
    """
    LOGGER.info(f"Marking duel deck status for {set_code}")
    if set_code.startswith("DD") or set_code in {"GS1"}:
        land_pile_marked = False
        side_letter_as_number = ord("a")

        for card in sorted(mtgjson_cards):
            if card.name in constants.BASIC_LAND_NAMES:
                land_pile_marked = True
            elif any(_type in card.type for _type in ("Token", "Emblem")):
                continue
            elif land_pile_marked:
                side_letter_as_number += 1
                land_pile_marked = False

            card.duel_deck = chr(side_letter_as_number)
    LOGGER.info(f"Finished marking duel deck status for {set_code}")


def parse_keyrune_code(url: str) -> str:
    """
    Convert a URL of a keyrune icon into its proper handle
    :param url: URL to keyrune to parse
    :return Proper keyrune code
    """
    file_stem = pathlib.Path(url).stem.upper()

    with constants.RESOURCE_PATH.joinpath("keyrune_code_overrides.json").open(
        encoding="utf-8"
    ) as file:
        upstream_to_keyrune_map: Dict[str, str] = json.load(file)

    return upstream_to_keyrune_map.get(file_stem, file_stem)


def get_translation_data(mtgjson_set_name: str) -> Optional[Dict[str, str]]:
    """
    Get translation data given a particular set name
    :param mtgjson_set_name: Set name to try and find in translation data
    :returns Translation data for the set, if found
    """
    with constants.RESOURCE_PATH.joinpath("mkm_set_name_translations.json").open(
        encoding="utf-8"
    ) as file:
        translation_data: Dict[str, Dict[str, str]] = json.load(file)

    return translation_data.get(mtgjson_set_name)


def build_mtgjson_set(set_code: str) -> Optional[MtgjsonSetObject]:
    """
    Construct a MTGJSON Magic Set
    :param set_code: Set to construct
    :return: Set object
    """
    # Ensure cache is initialized
    if GLOBAL_CACHE.cards_df is None:
        LOGGER.info("Initializing global cache...")
        GLOBAL_CACHE.load_all()

    # Output Object
    mtgjson_set = MtgjsonSetObject()

    # Attempt to load local set before getting from external provider
    additional_sets_data = load_local_set_data()
    set_data = additional_sets_data.get(
        set_code.upper(), get_scryfall_set_data(set_code)
    )
    if not set_data:
        return None

    # Explicit Variables
    mtgjson_set.name = set_data["name"].strip()
    mtgjson_set.code = set_data["code"].upper()
    mtgjson_set.type = set_data["set_type"]
    mtgjson_set.keyrune_code = parse_keyrune_code(set_data["icon_svg_uri"])
    mtgjson_set.release_date = set_data["released_at"]
    mtgjson_set.mtgo_code = set_data.get("mtgo_code", "").upper()
    mtgjson_set.parent_code = set_data.get("parent_set_code", "").upper()
    mtgjson_set.block = set_data.get("block", "")
    mtgjson_set.is_online_only = set_data.get("digital", "")
    mtgjson_set.is_foil_only = set_data.get("foil_only", "")
    mtgjson_set.is_non_foil_only = set_data.get("nonfoil_only", "")
    mtgjson_set.search_uri = set_data["search_uri"]
    if set_code.upper() not in additional_sets_data:
        mtgjson_set.languages = (
            ScryfallProviderSetLanguageDetector().get_set_printing_languages(
                mtgjson_set.code
            )
        )
    mtgjson_set.mcm_name = CardMarketProvider().get_set_name(mtgjson_set.name)
    mtgjson_set.mcm_id = CardMarketProvider().get_set_id(mtgjson_set.name)
    mtgjson_set.mcm_id_extras = CardMarketProvider().get_extras_set_id(mtgjson_set.name)
    mtgjson_set.translations = MtgjsonTranslationsObject(
        get_translation_data(mtgjson_set.name)
    )

    # Building cards is a process
    if mtgjson_set.code != "MB1":
        mtgjson_set.cards = build_base_mtgjson_cards(
            set_code, set_release_date=mtgjson_set.release_date
        )
    add_is_starter_option(set_code, mtgjson_set.search_uri, mtgjson_set.cards)
    add_rebalanced_to_original_linkage(mtgjson_set)
    relocate_miscellaneous_tokens(mtgjson_set)

    if mtgjson_set.code in {"CN2", "FRF", "ONS", "10E", "UNH"}:
        link_same_card_different_details(mtgjson_set)

    if mtgjson_set.code in {"EMN", "BRO"}:
        add_meld_face_parts(mtgjson_set)

    if mtgjson_set.code in {"SLD"}:
        add_secret_lair_names(mtgjson_set)

    base_total_sizes = get_base_and_total_set_sizes(mtgjson_set)
    mtgjson_set.base_set_size = base_total_sizes[0]
    mtgjson_set.total_set_size = base_total_sizes[1]

    add_other_face_ids(mtgjson_set.cards)
    add_variations_and_alternative_fields(mtgjson_set)

    # Build tokens, a little less of a process
    mtgjson_set.tokens = build_base_mtgjson_tokens(
        f"T{set_code}", mtgjson_set.extra_tokens or []
    )
    if mtgjson_set.tokens:
        mtgjson_set.token_set_code = mtgjson_set.tokens[0].set_code

    add_other_face_ids(mtgjson_set.tokens)
    add_mcm_details(mtgjson_set)
    add_card_kingdom_details(mtgjson_set)

    with RESOURCE_PATH.joinpath("tcgplayer_set_id_overrides.json").open(
        encoding="utf-8"
    ) as fp:
        tcgplayer_set_id_overrides: Dict[str, int] = json.load(fp)
    if tcgplayer_set_id_overrides.get(mtgjson_set.code):
        mtgjson_set.tcgplayer_group_id = tcgplayer_set_id_overrides[mtgjson_set.code]
    else:
        mtgjson_set.tcgplayer_group_id = set_data.get("tcgplayer_id")

    mtgjson_set.booster = GitHubBoostersProvider().get_set_booster_data(set_code)

    mtgjson_set.sealed_product = GitHubSealedProvider().get_sealed_products_data(
        set_code
    )
    CardKingdomProvider().update_sealed_urls(mtgjson_set.sealed_product)
    GitHubSealedProvider().apply_sealed_contents_data(
        set_code, mtgjson_set.sealed_product
    )
    add_sealed_uuid(mtgjson_set.sealed_product)
    TCGPlayerProvider().update_sealed_urls(mtgjson_set.sealed_product)
    add_sealed_purchase_url(mtgjson_set.sealed_product)

    add_token_signatures(mtgjson_set)

    add_multiverse_bridge_ids(mtgjson_set)

    mark_duel_decks(set_code, mtgjson_set.cards)

    mtgjson_set.decks = GitHubDecksProvider().get_decks_in_set(set_code)

    add_card_products_to_cards(mtgjson_set)

    if "Art Series" in mtgjson_set.name:
        add_orientations(mtgjson_set)

    # Implicit Variables
    mtgjson_set.is_foreign_only = mtgjson_set.code in constants.FOREIGN_SETS
    mtgjson_set.is_partial_preview = MtgjsonMetaObject().date < mtgjson_set.release_date

    apply_manual_overrides(mtgjson_set.cards)

    return mtgjson_set


def build_base_mtgjson_tokens(
    set_code: str, added_tokens: List[Dict[str, Any]]
) -> List[MtgjsonCardObject]:
    """
    Construct all tokens in MTGJSON format from a single set
    :param set_code: Set to build
    :param added_tokens: Additional tokens to build
    :return: Completed card objects
    """
    return build_base_mtgjson_cards(set_code, added_tokens, True)


def add_sealed_uuid(sealed_products: List[MtgjsonSealedProductObject]) -> None:
    """
    Adds all uuids to each sealed product object within a set
    :param sealed_products: Sealed products within the set
    """
    for sealed_product in sealed_products:
        add_uuid(sealed_product)


def add_sealed_purchase_url(sealed_products: List[MtgjsonSealedProductObject]) -> None:
    """
    Adds all purchase urls to each sealed product object within a set
    :param sealed_products: Sealed products within the set
    """
    for sealed_product in sealed_products:
        if sealed_product.identifiers.tcgplayer_product_id:
            sealed_product.raw_purchase_urls[
                "tcgplayer"
            ] = TCGPlayerProvider().product_url.format(
                sealed_product.identifiers.tcgplayer_product_id
            )

            sealed_product.purchase_urls.tcgplayer = url_keygen(
                sealed_product.identifiers.tcgplayer_product_id + sealed_product.uuid
            )
        if "cardKingdom" in sealed_product.raw_purchase_urls:
            sealed_product.purchase_urls.card_kingdom = url_keygen(
                sealed_product.raw_purchase_urls["cardKingdom"]
            )


def build_base_mtgjson_cards(
    set_code: str,
    additional_cards: Optional[List[Dict[str, Any]]] = None,
    is_token: bool = False,
    set_release_date: str = "",
) -> List[MtgjsonCardObject]:
    """
    Construct all cards in MTGJSON format from a single set
    :param set_code: Set to build
    :param additional_cards: Additional objs to build (not relevant for normal builds)
    :param is_token: Are tokens being compiled?
    :param set_release_date: Original set release date
    :return: Completed card objects
    """
    LOGGER.info(f"Building cards for {set_code}")

    if GLOBAL_CACHE.cards_df is None:
        raise RuntimeError("Cache not initialized - call cache.load_all() first")

    # Get English cards for this set from cache
    cards = GLOBAL_CACHE.cards_df.filter(
        (pl.col("set") == set_code.upper()) & (pl.col("lang") == "en")
    ).to_dicts()

    cards.extend(additional_cards or [])

    mtgjson_cards: List[MtgjsonCardObject] = parallel_call(
        build_mtgjson_card,
        cards,
        fold_list=True,
        repeatable_args=(0, is_token, set_release_date),
    )

    # Ensure we have a consistent ordering for our outputs
    mtgjson_cards.sort()

    LOGGER.info(f"Finished building cards for {set_code}")
    return mtgjson_cards


def add_is_starter_option(
    set_code: str, _search_url: str, mtgjson_cards: List[MtgjsonCardObject]
) -> None:
    """
    There are cards that may not exist in standard boosters. As such, we mark
    those as starter cards.
    :param set_code: Set to handle
    :param _search_url: URL to search for cards in (unused with cache)
    :param mtgjson_cards: Card Objects to modify
    """
    LOGGER.info(f"Add starter data to {set_code}")

    if GLOBAL_CACHE.cards_df is None:
        LOGGER.warning(f"Cache not loaded, skipping starter data for {set_code}")
        return

    # Get scryfall IDs for non-booster cards in this set
    starter_ids = set(
        GLOBAL_CACHE.cards_df.filter(
            (pl.col("set") == set_code.upper()) & ~pl.col("booster")
        )["id"].to_list()
    )

    if not starter_ids:
        LOGGER.debug(f"All cards in {set_code} are available in boosters")
        LOGGER.info(f"Finished adding starter data to {set_code}")
        return

    for card in mtgjson_cards:
        if card.identifiers.scryfall_id in starter_ids:
            card.is_starter = True

    LOGGER.info(f"Finished adding starter data to {set_code}")


def add_leadership_skills(mtgjson_card: MtgjsonCardObject) -> None:
    """Determine if a card is able to be your commander."""
    override_cards = ("Grist, the Hunger Tide",)
    card_text = mtgjson_card.text or ""
    card_type = mtgjson_card.type or ""

    is_commander_legal = (
        mtgjson_card.name in override_cards
        or (
            "Legendary" in card_type
            and (
                "Creature" in card_type
                or (
                    ("Vehicle" in card_type or "Spacecraft" in card_type)
                    and mtgjson_card.toughness
                    and mtgjson_card.power
                )
            )
            and card_type not in {"flip"}
            and (mtgjson_card.side == "a" if mtgjson_card.side else True)
        )
        or ("can be your commander" in card_text)
    )

    is_oathbreaker_legal = "Planeswalker" in card_type

    is_brawl_legal = (
        mtgjson_card.set_code.upper() in WhatsInStandardProvider().set_codes
        and (is_oathbreaker_legal or is_commander_legal)
    )

    if is_commander_legal or is_oathbreaker_legal or is_brawl_legal:
        mtgjson_card.leadership_skills = MtgjsonLeadershipSkillsObject(
            is_brawl_legal, is_commander_legal, is_oathbreaker_legal
        )


def get_mtgjson_v4_uuid(mtgjson_object: MtgjsonCardObject) -> str:
    """
    MTGJSONv4's UUID generation method
    """
    if {"Token", "Card"}.intersection(mtgjson_object.types):
        # Tokens have a special generation method
        id_source_v4 = (
            (
                mtgjson_object.face_name
                if mtgjson_object.face_name
                else mtgjson_object.name
            )
            + "".join((mtgjson_object.colors or ""))
            + (mtgjson_object.power or "")
            + (mtgjson_object.toughness or "")
            + (mtgjson_object.side or "")
            + mtgjson_object.set_code[1:].upper()
            + (mtgjson_object.identifiers.scryfall_id or "")
        )
    else:
        # Normal cards only need a few pieces of data
        id_source_v4 = (
            "sf"
            + (mtgjson_object.identifiers.scryfall_id or "")
            + (
                mtgjson_object.face_name
                if mtgjson_object.face_name
                else mtgjson_object.name
            )
        )

    return str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v4))


def add_uuid(
    mtgjson_object: Union[MtgjsonCardObject, MtgjsonSealedProductObject],
) -> None:
    """
    Construct a UUIDv5 for each MTGJSON card object
    This will also add UUIDv4 for legacy support
    :param mtgjson_object: Card object
    """
    if isinstance(mtgjson_object, MtgjsonSealedProductObject):
        mtgjson_object.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, mtgjson_object.name))
        return

    mtgjson_object.identifiers.mtgjson_v4_id = get_mtgjson_v4_uuid(mtgjson_object)

    id_source_v5 = str(mtgjson_object.identifiers.scryfall_id) + (
        mtgjson_object.side or "a"
    )
    add_extra_language_uuids(mtgjson_object, id_source_v5)

    # MTGJSONv5 UUIDs will now be generated using
    # Scryfall_UUID + Side ("a" by default if side not specified).
    # For UUIDs that have been generated in the past, we will continue
    # to honor those and, instead of regenerating them, will load them
    # from a backup cache
    cached_mtgjson_uuid = UuidCacheProvider().get_uuid(
        str(mtgjson_object.identifiers.scryfall_id), (mtgjson_object.side or "a")
    )
    if cached_mtgjson_uuid:
        mtgjson_object.uuid = cached_mtgjson_uuid
        return

    mtgjson_object.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v5))


def add_extra_language_uuids(
    mtgjson_card: MtgjsonCardObject, id_source_prefix: str
) -> None:
    """
    Add unique identifiers to each language's printing of a card
    """
    for language_entry in mtgjson_card.foreign_data:
        language_entry.uuid = str(
            uuid.uuid5(
                uuid.NAMESPACE_DNS, id_source_prefix + "_" + language_entry.language
            )
        )


def build_mtgjson_card(
    scryfall_object: dict[str, Any],
    face_id: int = 0,
    is_token: bool = False,
    set_release_date: str = "",
) -> list[MtgjsonCardObject]:
    """Build MTGJSON card(s) from Scryfall object."""

    ctx = CardBuildContext(
        scryfall=scryfall_object,
        face_id=face_id,
        is_token=is_token,
        set_release_date=set_release_date,
    )

    LOGGER.info(f"Building {ctx.scryfall['set'].upper()}: {ctx.scryfall['name']}")

    # Build this face
    card = MtgjsonCardObject(is_token)

    _set_basic_info(card, ctx)
    _set_identifiers(card, ctx)
    _set_face_data(card, ctx)
    _set_card_attributes(card, ctx)
    _set_types_and_text(card, ctx)
    _set_legalities_and_rulings(card, ctx)
    _set_availability(card, ctx)
    _finalize_card(card, ctx)
    _set_purchase_urls(card, ctx)

    # Collect results
    results = [card]

    # Recursively build other faces
    if ctx.face_id == 0 and ctx.is_multi_face:
        for i in range(1, ctx.face_count):
            results.extend(
                build_mtgjson_card(scryfall_object, i, is_token, set_release_date)
            )

    return results


def _set_basic_info(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set basic card info: name, language, set."""
    sf = ctx.scryfall

    card.name = sf["name"]
    card.set_code = sf["set"].upper()
    card.language = constants.LANGUAGE_MAP.get(sf["lang"], "unknown")

    # Flavor/printed names
    card.flavor_name = sf.get("flavor_name") or sf.get("printed_name")
    card.printed_name = sf.get("printed_name")
    card.printed_type = sf.get("printed_type_line")
    card.printed_text = sf.get("printed_text")


def _set_identifiers(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set all card identifiers."""
    sf = ctx.scryfall
    face = ctx.face_data

    card.identifiers.scryfall_id = sf["id"]

    # Oracle ID - check face first for split cards
    card.identifiers.scryfall_oracle_id = sf.get("oracle_id") or face.get("oracle_id")

    card.identifiers.scryfall_illustration_id = sf.get("illustration_id") or face.get(
        "illustration_id"
    )
    card.identifiers.scryfall_card_back_id = sf.get("card_back_id")

    # External IDs
    card.identifiers.mcm_id = get_str_or_none(sf.get("cardmarket_id"))
    card.identifiers.mtg_arena_id = get_str_or_none(sf.get("arena_id"))
    card.identifiers.mtgo_id = get_str_or_none(sf.get("mtgo_id"))
    card.identifiers.mtgo_foil_id = get_str_or_none(sf.get("mtgo_foil_id"))

    # Multiverse ID
    multiverse_ids = sf.get("multiverse_ids") or []
    if multiverse_ids:
        idx = ctx.face_id if ctx.face_id < len(multiverse_ids) else 0
        card.identifiers.multiverse_id = get_str_or_none(multiverse_ids[idx])


def _set_face_data(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Handle multi-face card specifics."""
    if not ctx.is_multi_face:
        return

    sf = ctx.scryfall
    face = ctx.face_data
    faces = sf["card_faces"]

    # Set names for all faces
    card.set_names(sf["name"].split("//"))

    # Illustration IDs for all faces
    card.set_illustration_ids([f.get("illustration_id", "Missing") for f in faces])

    # Face-specific flavor name
    if face.get("flavor_name"):
        card.flavor_name = " // ".join(
            f.get("flavor_name", face["flavor_name"]) for f in faces
        )
        card.face_flavor_name = face["flavor_name"]

    if face.get("printed_name"):
        card.face_flavor_name = face["printed_name"]
        card.face_printed_name = face["printed_name"]

    # Mana value handling per layout
    _set_face_mana_value(card, ctx)

    # Artist from face
    card.artist = face.get("artist", "")
    card.artist_ids = face.get("artist_ids", "")

    # Watermark from first face
    card.set_watermark(faces[0].get("watermark"))

    # Check for aftermath
    if faces[-1].get("oracle_text", "").startswith("Aftermath"):
        card.layout = "aftermath"

    # Set face name and side
    _set_face_name_and_side(card, ctx)


def _set_face_mana_value(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Calculate face mana value based on layout."""
    sf = ctx.scryfall
    face = ctx.face_data
    layout = sf.get("layout") or ""
    mana_cost = sf.get("mana_cost") or ""

    # Split mana cost (e.g., "{1}{W} // {2}{U}")
    if "//" in mana_cost:
        parts = mana_cost.split("//")
        if ctx.face_id < len(parts):
            card.colors = get_card_colors(parts[ctx.face_id])
            card.face_mana_value = get_card_cmc(parts[ctx.face_id])
            card.face_converted_mana_cost = card.face_mana_value

    elif layout in {"split", "transform", "aftermath", "adventure"}:
        card.face_mana_value = get_card_cmc(face.get("mana_cost") or "0")
        card.face_converted_mana_cost = card.face_mana_value

    elif layout == "modal_dfc":
        card.mana_value = get_card_cmc(face.get("mana_cost") or "0")
        card.face_mana_value = card.mana_value
        card.converted_mana_cost = card.mana_value
        card.face_converted_mana_cost = card.mana_value

    elif layout == "reversible_card":
        card.mana_value = face.get("cmc") or 0
        card.converted_mana_cost = card.mana_value


def _set_face_name_and_side(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Determine face_name and side letter."""
    face = ctx.face_data
    face_names = card.get_names()

    if not face_names:
        return

    card.face_name = str(face.get("name", ""))

    if card.layout == "meld":
        return  # Meld handled separately

    # Special case: TUST tokens
    if card.set_code.upper() == "tust":
        face_type = face.get("type_line", "")
        card.side = "a" if "Token" not in face_type else "b"
        return

    # Art series: all faces have same name
    if face_names.count(face_names[0]) == len(face_names):
        illustration_ids = card.get_illustration_ids()

        if len(set(illustration_ids)) == 1:
            # Same illustration - use face index
            card.side = chr(ctx.face_id + 97)
        else:
            # Match by illustration
            for i, ill_id in enumerate(illustration_ids):
                if ill_id == card.identifiers.scryfall_illustration_id:
                    card.side = chr(i + 97)
                    break

        # Fallback for missing illustrations
        if not card.side and "Missing" in illustration_ids:
            card.side = chr(ctx.face_id + 97)
        return

    # ADSK special case
    if card.set_code.upper() == "adsk":
        card.side = chr(ctx.face_id + 97)
        return

    # Standard: match face name to position
    if card.face_name in face_names:
        card.side = chr(face_names.index(card.face_name) + 97)


def _set_card_attributes(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set card attributes from Scryfall data."""
    sf = ctx.scryfall
    face = ctx.face_data

    if face.get("mana_cost"):
        card.mana_cost = face["mana_cost"]

    if not card.colors:
        card.colors = face.get("colors") or sf.get("colors") or []

    card.border_color = sf.get("border_color") or ""
    card.color_identity = sf.get("color_identity") or []
    card.color_indicator = face.get("color_indicator") or sf.get("color_indicator")

    if not hasattr(card, "mana_value") or not card.mana_value:
        card.mana_value = sf.get("cmc") or 0.0
        card.converted_mana_cost = sf.get("cmc") or 0.0

    card.number = sf.get("collector_number") or "0"
    card.rarity = sf.get("rarity") or ""

    card.frame_version = sf.get("frame") or ""
    card.frame_effects = sorted(sf.get("frame_effects") or [])
    card.security_stamp = sf.get("security_stamp")

    if not card.artist:
        card.artist = sf.get("artist") or ""
    if not card.artist_ids:
        card.artist_ids = sf.get("artist_ids") or []
    if not card.watermark:
        card.set_watermark(face.get("watermark"))

    card.finishes = sf.get("finishes") or []
    card.has_foil = any(f in card.finishes for f in ("foil", "glossy"))
    card.has_non_foil = "nonfoil" in card.finishes

    # Boolean flags
    card.has_content_warning = sf.get("content_warning")
    card.is_full_art = sf.get("full_art")
    card.is_game_changer = sf.get("game_changer")
    card.is_online_only = sf.get("digital")
    card.is_oversized = sf.get("oversized") or card.set_code in ("OC21",)
    card.is_promo = sf.get("promo")
    card.is_reprint = sf.get("reprint")
    card.is_reserved = sf.get("reserved")
    card.is_story_spotlight = sf.get("story_spotlight")
    card.is_textless = sf.get("textless")

    # Stats
    card.loyalty = face.get("loyalty")
    card.defense = face.get("defense")
    card.power = face.get("power", "")
    card.toughness = face.get("toughness", "")
    card.hand = sf.get("hand_modifier")
    card.life = sf.get("life_modifier")

    # EDHREC
    card.edhrec_rank = sf.get("edhrec_rank")
    card.edhrec_saltiness = EdhrecProviderCardRanks().get_salt_rating(
        card.name.split("/")[0].strip() if "/" in card.name else card.name
    )

    # Promo types
    card.promo_types = sf.get("promo_types") or []
    if card.number.endswith("p"):
        card.promo_types.append("planeswalkerstamped")
    card.promo_types = [t for t in card.promo_types if t not in {"planeswalkerdeck"}]

    card.booster_types = []
    if sf.get("booster", False):
        card.booster_types.append("default")
    if any(d in card.promo_types for d in ("starterdeck", "planeswalkerdeck")):
        card.booster_types.append("deck")

    card.attraction_lights = sf.get("attraction_lights")

    card_release = sf.get("released_at")
    if ctx.set_release_date and ctx.set_release_date != card_release:
        card.original_release_date = card_release


def _set_types_and_text(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set type line, oracle text, and parsed types."""
    sf = ctx.scryfall
    face = ctx.face_data

    card.type = face.get("type_line", "Card")
    card.text = face.get("oracle_text", "")
    card.flavor_text = face.get("flavor_text") or sf.get("flavor_text")

    # Layout
    if sf.get("layout") == "art_series":
        card.layout = "art_series"
    elif "//" not in card.name and any(
        t in sf.get("type_line", "").lower() for t in ("card", "token")
    ):
        card.layout = "token"
    if not card.layout:
        card.layout = sf.get("layout", "")

    # Parse types
    supertypes, types, subtypes = parse_card_types(card.type)
    card.supertypes = supertypes
    card.types = types
    card.subtypes = subtypes

    # Planeswalker text formatting
    if "Planeswalker" in card.types:
        card.text = re.sub(r"([+−-]?[0-9X]+):", r"[\1]:", card.text)

    # Keywords (only those in this face's text)
    card.keywords = [
        kw for kw in sorted(sf.get("keywords") or []) if kw.lower() in card.text.lower()
    ]

    # ASCII name
    ascii_name = (
        unicodedata.normalize("NFD", card.name).encode("ascii", "ignore").decode()
    )
    if card.name != ascii_name:
        card.ascii_name = ascii_name


def _set_legalities_and_rulings(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set legalities, rulings, and printings from cache."""
    sf = ctx.scryfall

    # Legalities
    legalities = sf.get("legalities") or {}
    if sf.get("set_type") == "memorabilia":
        legalities = {}
    card.legalities = parse_legalities(legalities)

    # Printings and rulings from cache
    oracle_id = card.identifiers.scryfall_oracle_id
    card.printings = parse_printings(oracle_id)
    card.rulings = parse_rulings(oracle_id)

    # Foreign data from cache
    card.foreign_data = parse_foreign(
        card.set_code, card.number, card.face_name if card.face_name else card.name
    )

    # Implicit flags
    card.is_funny = sf.get("set_type") == "funny" and (
        card.security_stamp == "acorn" if card.set_code == "UNF" else True
    )
    card.is_timeshifted = sf.get("frame") == "future" or card.set_code.upper() == "tsb"

    # Rebalanced cards
    if card.name.startswith("A-"):
        card.is_alternative = True
        card.is_rebalanced = True


def _set_availability(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set platform availability."""
    sf = ctx.scryfall
    games = sf.get("games") or []

    card.availability = MtgjsonGameFormatsObject()
    card.availability.arena = (
        "arena" in games or card.identifiers.mtg_arena_id is not None
    )
    card.availability.mtgo = "mtgo" in games or card.identifiers.mtgo_id is not None
    card.availability.paper = not card.is_online_only
    card.availability.shandalar = "astral" in games
    card.availability.dreamcast = "sega" in games


def _set_purchase_urls(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Set purchase URLs and related identifiers."""
    sf = ctx.scryfall

    card.raw_purchase_urls.update(sf.get("purchase_uris") or {})
    card.raw_purchase_urls.pop("tcgplayer", None)

    if "tcgplayer_id" in sf:
        product_id = str(sf["tcgplayer_id"])
        card.identifiers.tcgplayer_product_id = product_id
        card.purchase_urls.tcgplayer = url_keygen(product_id + card.uuid)
        card.raw_purchase_urls["tcgplayer"] = TCGPlayerProvider().product_url.format(
            product_id
        )

    if "tcgplayer_etched_id" in sf:
        etched_id = str(sf["tcgplayer_etched_id"])
        card.identifiers.tcgplayer_etched_product_id = etched_id
        card.purchase_urls.tcgplayer_etched = url_keygen(etched_id + card.uuid)
        card.raw_purchase_urls["tcgplayerEtched"] = (
            TCGPlayerProvider().product_url.format(etched_id)
        )


def _finalize_card(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Final card processing: UUID, leadership, meld, gatherer."""
    sf = ctx.scryfall

    # Handle meld and all_parts
    _handle_all_parts(card, ctx)

    # Alternative deck limit
    if card.name in ScryfallProvider().cards_without_limits:
        card.has_alternative_deck_limit = True

    # UUID and leadership
    add_uuid(card)
    add_leadership_skills(card)

    # Related cards
    add_related_cards(sf, card, ctx.is_token)

    # Gatherer original text/type
    if card.identifiers.multiverse_id:
        gatherer_cards = GathererProvider().get_cards(card.identifiers.multiverse_id)
        if gatherer_cards and isinstance(gatherer_cards, list):
            card.original_type = gatherer_cards[0].get("original_types")
            card.original_text = gatherer_cards[0].get("original_text")


def _handle_all_parts(card: MtgjsonCardObject, ctx: CardBuildContext) -> None:
    """Handle meld cards and all_parts relationships."""
    sf = ctx.scryfall

    if not sf.get("all_parts"):
        return

    card.set_names(None)

    for part in sorted(sf["all_parts"], key=lambda p: p["component"]):
        component = part["component"]

        if component == "token":
            continue

        if component.startswith("meld"):
            card.append_names(part.get("name"))
            continue

        # Handle double-faced rebalanced cards
        if card.name in part.get("name", "") and "//" in part.get("name", ""):
            card.set_names(part["name"].split("//"))
            break

    # Clear if only self-reference
    names = card.get_names()
    if names and len(names) == 1 and card.name in names:
        card.set_names(None)
        return

    # Meld triplet handling
    if names and len(names) == 3:
        _finalize_meld(card)


def _finalize_meld(card: MtgjsonCardObject) -> None:
    """Finalize meld card names and sides."""
    for card_a, card_b, meld_c in GLOBAL_CACHE.meld_triplets:
        if card_a in card.get_names():
            card.set_names([card_a, card_b, meld_c])
            break

    names = card.get_names()
    mana_val = card.mana_value or 0
    card.face_mana_value = mana_val
    card.face_converted_mana_cost = mana_val

    if card.name != names[2]:
        card.side = "a"
        card.face_name = card.name
        card.name = f"{card.name} // {names[2]}"
    else:
        card.side = "b"
        card.face_name = card.name


def add_variations_and_alternative_fields(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Set the variations and is_alternative
    statuses for all cards within the set object.
    :param mtgjson_set: MTGJSON Set Object to modify
    """
    if not mtgjson_set.cards:
        return

    LOGGER.info(f"Adding variations for {mtgjson_set.code}")

    distinct_card_printings_found: Set[str] = set()
    for this_card in mtgjson_set.cards:
        # Adds variations
        variations = [
            item.uuid
            for item in mtgjson_set.cards
            if item.name.split(" (")[0] == this_card.name.split(" (")[0]
            and item.face_name == this_card.face_name
            and item.uuid != this_card.uuid
            and (item.number != this_card.number if item.number else True)
        ]

        if variations:
            this_card.variations = variations

        # Add alternative tag
        # Ignore singleton printings in set, as well as basics
        if not variations or this_card.name in constants.BASIC_LAND_NAMES:
            continue

        # In each set, a card has to be unique by all of these attributes
        # otherwise, it's an alternative printing
        distinct_card_printing = (
            f"{this_card.name}|{this_card.border_color}|{this_card.frame_version}|"
            f"{','.join(this_card.frame_effects)}|{this_card.side}"
        )
        if this_card.set_code in {"UNH", "10E"}:
            distinct_card_printing += f"|{','.join(this_card.finishes)}"

        if distinct_card_printing in distinct_card_printings_found:
            this_card.is_alternative = True
        else:
            distinct_card_printings_found.add(distinct_card_printing)

    LOGGER.info(f"Finished adding variations for {mtgjson_set.code}")


def add_other_face_ids(cards_to_act_on: List[MtgjsonCardObject]) -> None:
    """
    Add other face IDs to all cards within a group based on
    that group. If a duplicate is found, the cards will link
    via other_face_ids
    :param cards_to_act_on: Cards to find duplicates of in the group
    """
    if not cards_to_act_on:
        return

    LOGGER.info("Adding otherFaceIds to group")
    for this_card in cards_to_act_on:
        # Adds other face ID list
        if this_card.get_names():
            this_card.other_face_ids = []
            for other_card in cards_to_act_on:
                if other_card.face_name not in this_card.get_names():
                    continue

                if other_card.uuid == this_card.uuid:
                    continue

                if this_card.layout == "meld":
                    # Meld cards should account for the other sides
                    if this_card.side != other_card.side:
                        this_card.other_face_ids.append(other_card.uuid)
                elif other_card.number:
                    # Most split cards should have the same number
                    if other_card.number == this_card.number:
                        this_card.other_face_ids.append(other_card.uuid)
                else:
                    # No number? No problem, just add it!
                    this_card.other_face_ids.append(other_card.uuid)
    LOGGER.info("Finished adding otherFaceIds to group")


def link_same_card_different_details(mtgjson_set: MtgjsonSetObject) -> None:
    """
    In several Magic sets, the foil and non-foil printings have different text
    (See 10th Edition, for example). If that's the case, we will link the
    Foil and NonFoil versions together in the identifiers for easier user management
    :param mtgjson_set: MTGJSON Set
    """
    LOGGER.info(f"Linking multiple printings for {mtgjson_set.code}")
    cards_seen = {}

    for mtgjson_card in mtgjson_set.cards:
        if mtgjson_card.identifiers.scryfall_illustration_id not in cards_seen:
            cards_seen[mtgjson_card.identifiers.scryfall_illustration_id] = mtgjson_card
            continue

        other_mtgjson_card = cards_seen[
            mtgjson_card.identifiers.scryfall_illustration_id
        ]
        if "nonfoil" in mtgjson_card.finishes:
            other_mtgjson_card.identifiers.mtgjson_non_foil_version_id = (
                mtgjson_card.uuid
            )
            mtgjson_card.identifiers.mtgjson_foil_version_id = other_mtgjson_card.uuid
        else:
            other_mtgjson_card.identifiers.mtgjson_foil_version_id = mtgjson_card.uuid
            mtgjson_card.identifiers.mtgjson_non_foil_version_id = (
                other_mtgjson_card.uuid
            )


def add_card_kingdom_details(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Add the CardKingdom components, like IDs and purchase URLs
    :param mtgjson_set: MTGJSON Set
    """
    LOGGER.info(f"Adding CK details for {mtgjson_set.code}")
    if GLOBAL_CACHE.card_kingdom_map is None:
        LOGGER.warning("Card Kingdom cache not loaded")
        return

    translation_table = GLOBAL_CACHE.card_kingdom_map
    if not translation_table:
        LOGGER.warning("Card Kingdom cache not loaded")
        return

    for mtgjson_card in mtgjson_set.cards + mtgjson_set.tokens:
        if mtgjson_card.identifiers.scryfall_id not in translation_table:
            continue

        entries = translation_table[mtgjson_card.identifiers.scryfall_id]
        for entry in entries:
            if "Foil Etched" in entry.get("variation", ""):
                mtgjson_card.identifiers.card_kingdom_etched_id = str(entry["id"])
                mtgjson_card.purchase_urls.card_kingdom_etched = url_keygen(
                    CardKingdomProvider().url_prefix + entry["url"] + mtgjson_card.uuid
                )
                mtgjson_card.raw_purchase_urls.update(
                    {
                        "cardKingdomEtched": CardKingdomProvider().url_prefix
                        + entry["url"]
                        + constants.CARD_KINGDOM_REFERRAL
                    }
                )
            elif entry.get("is_foil", "false") == "true":
                mtgjson_card.identifiers.card_kingdom_foil_id = str(entry["id"])
                mtgjson_card.purchase_urls.card_kingdom_foil = url_keygen(
                    CardKingdomProvider().url_prefix + entry["url"] + mtgjson_card.uuid
                )
                mtgjson_card.raw_purchase_urls.update(
                    {
                        "cardKingdomFoil": CardKingdomProvider().url_prefix
                        + entry["url"]
                        + constants.CARD_KINGDOM_REFERRAL
                    }
                )
            else:
                mtgjson_card.identifiers.card_kingdom_id = str(entry["id"])
                mtgjson_card.purchase_urls.card_kingdom = url_keygen(
                    CardKingdomProvider().url_prefix + entry["url"] + mtgjson_card.uuid
                )
                mtgjson_card.raw_purchase_urls.update(
                    {
                        "cardKingdom": CardKingdomProvider().url_prefix
                        + entry["url"]
                        + constants.CARD_KINGDOM_REFERRAL
                    }
                )

    LOGGER.info(f"Finished adding CK details for {mtgjson_set.code}")


def add_token_signatures(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Assign signatures to cards/tokens for sets that have
    artists sign the cards that are in mass print
    :param mtgjson_set: MTGJSON Set
    """

    def add_signature(card: MtgjsonCardObject, sig: str) -> None:
        """
        Private Method Signature adder, to keep consistent
        """
        card.signature = sig
        if "signed" not in card.finishes:
            card.finishes.append("signed")

    LOGGER.info(f"Adding signatures to cards for {mtgjson_set.code}")
    if mtgjson_set.name.endswith("Art Series") and mtgjson_set.code != "MH1":
        # All Art Series (except MH1) have signature options, up to this point
        for mtgjson_card in mtgjson_set.tokens:
            add_signature(mtgjson_card, mtgjson_card.artist)
    elif mtgjson_set.type == "memorabilia":
        # Gold Border Memorabilia sets contain signatures
        for mtgjson_cards in [mtgjson_set.tokens, mtgjson_set.cards]:
            for mtgjson_card in mtgjson_cards:
                if mtgjson_card.border_color != "gold":
                    continue

                signature = get_signature_from_number(mtgjson_card)
                if signature:
                    add_signature(mtgjson_card, signature)

    LOGGER.info(f"Finished adding signatures to cards for {mtgjson_set.code}")


def add_multiverse_bridge_ids(mtgjson_set: MtgjsonSetObject) -> None:
    """
    There are extra IDs that can be useful for the community to have
    knowledge of. This step will incorporate all of those IDs
    """
    LOGGER.info(f"Adding MultiverseBridge details for {mtgjson_set.code}")

    rosetta_stone_cards = GLOBAL_CACHE.multiverse_bridge_cards
    if not rosetta_stone_cards:
        LOGGER.warning("MultiverseBridge cache not loaded")
        return

    for mtgjson_card in mtgjson_set.cards:
        if mtgjson_card.identifiers.scryfall_id not in rosetta_stone_cards:
            LOGGER.info(
                f"MultiverseBridge missing {mtgjson_card.name} in {mtgjson_card.set_code}"
            )
            continue

        for rosetta_card_print in rosetta_stone_cards.get(
            mtgjson_card.identifiers.scryfall_id, []
        ):
            attr = (
                "cardsphere_foil_id"
                if rosetta_card_print.get("is_foil")
                else "cardsphere_id"
            )
            setattr(mtgjson_card.identifiers, attr, str(rosetta_card_print["cs_id"]))
            if rosetta_card_print["deckbox_id"]:
                setattr(
                    mtgjson_card.identifiers,
                    "deckbox_id",
                    str(rosetta_card_print["deckbox_id"]),
                )

    mtgjson_set.cardsphere_set_id = (
        MultiverseBridgeProvider()
        .get_rosetta_stone_sets()
        .get(mtgjson_set.code.upper())
    )


def add_mcm_details(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Add the MKM components to a set's cards and tokens
    :param mtgjson_set: MTGJSON Set
    """
    LOGGER.info(f"Adding MCM details for {mtgjson_set.code}")
    mkm_cards = CardMarketProvider().get_mkm_cards(mtgjson_set.mcm_id)

    extras_cards: Dict[str, List[Dict[str, Any]]] = {}
    if mtgjson_set.mcm_id_extras:
        extras_cards = CardMarketProvider().get_mkm_cards(mtgjson_set.mcm_id_extras)

    for mtgjson_card in mtgjson_set.cards + mtgjson_set.tokens:
        delete_key = False

        # "boosterfun" is an alias for frame_effects=showcase, frame_effects=extendedart, and border_color=borderless
        if "boosterfun" in mtgjson_card.promo_types and extras_cards:
            # It is an extended art, showcase, or borderless, search in the "Extras" set instead
            search_cards = extras_cards
        else:
            search_cards = mkm_cards

        # There are multiple ways MKM represents cards...
        if mtgjson_card.name.lower() in search_cards:
            # First lets see if the card name is found
            card_key = mtgjson_card.name.lower()
        elif mtgjson_card.face_name and mtgjson_card.face_name.lower() in search_cards:
            # If that failed, lets see if the face name is found
            card_key = mtgjson_card.face_name.lower()
        elif mtgjson_card.name.replace("//", "/").lower() in search_cards:
            # Finally, lets check if they used a single slash for split-type cards
            card_key = mtgjson_card.name.replace("//", "/").lower()
        elif (
            mtgjson_card.is_token
            and f"{mtgjson_card.name.lower()} token" in search_cards
        ):
            # Tokens usually end in the word token
            card_key = f"{mtgjson_card.name.lower()} token"
        else:
            # Multiple printings of a card in the set... just guess at this point
            card_key = ""
            for mkm_card in search_cards:
                if mkm_card.startswith(mtgjson_card.name.lower()):
                    card_key = mkm_card
                    delete_key = True
                    break

            if not card_key:
                LOGGER.debug(f"Failed to find {mtgjson_card.name} for MKM")
                continue

        if delete_key:
            del search_cards[card_key]

        if not search_cards[card_key]:
            continue

        for mkm_obj in search_cards[card_key]:
            if mtgjson_card.number in mkm_obj["number"]:
                break
        else:
            mkm_obj = search_cards[card_key][0]

        # This value is set by an upstream provider by default
        if not mtgjson_card.identifiers.mcm_id:
            mtgjson_card.identifiers.mcm_id = str(mkm_obj["idProduct"])

        mtgjson_card.identifiers.mcm_meta_id = str(mkm_obj["idMetaproduct"])

        mtgjson_card.purchase_urls.cardmarket = url_keygen(
            mtgjson_card.identifiers.mcm_id
            + mtgjson_card.uuid
            + constants.CARD_MARKET_BUFFER
            + mtgjson_card.identifiers.mcm_meta_id
        )

    LOGGER.info(f"Finished adding MCM details for {mtgjson_set.code}")


def get_base_and_total_set_sizes(mtgjson_set: MtgjsonSetObject) -> Tuple[int, int]:
    """
    Get the size of a set from cache or corrections file
    :param mtgjson_set: Mtgjson Set Object
    :return: Amount of cards in set (base, total)
    """
    # Load override file
    with constants.RESOURCE_PATH.joinpath("base_set_sizes.json").open(
        encoding="utf-8"
    ) as f:
        base_set_size_override = json.load(f)

    base_set_size = len(mtgjson_set.cards)

    if mtgjson_set.code.upper() in base_set_size_override.keys():
        # Manual correction
        base_set_size = int(base_set_size_override[mtgjson_set.code.upper()])
    else:
        # Use knowledge of Boosterfun being the first non-numbered card
        # in the set to identify the true base set size
        # BoosterFun started with Throne of Eldraine in Oct 2019
        if mtgjson_set.release_date > "2019-10-01":
            for card in mtgjson_set.cards:
                if "boosterfun" in card.promo_types:
                    card_number = re.findall(r"([0-9]+)", card.number)[0]
                    base_set_size = int(card_number) - 1
                    break
        else:
            # Count cards in base set from cache
            if GLOBAL_CACHE.cards_df is not None:
                base_set_count = GLOBAL_CACHE.cards_df.filter(
                    (pl.col("set") == mtgjson_set.code.upper())
                    & (pl.col("lang") == "en")
                    & (pl.col("booster"))
                ).height

                if base_set_count > 0:
                    base_set_size = base_set_count
                else:
                    # Fallback: count all English cards in set
                    base_set_size = GLOBAL_CACHE.cards_df.filter(
                        (pl.col("set") == mtgjson_set.code.upper())
                        & (pl.col("lang") == "en")
                    ).height

    total_set_size = sum(
        1 for card in mtgjson_set.cards if not getattr(card, "is_rebalanced", False)
    )
    return base_set_size, total_set_size


def get_signature_from_number(mtgjson_card: MtgjsonCardObject) -> Optional[str]:
    """
    Find the name of the person who signed a World Championship card
    :param mtgjson_card: Card object to get required data from
    :returns Name of person who signed card, if applicable
    """
    with constants.RESOURCE_PATH.joinpath("world_championship_signatures.json").open(
        encoding="utf-8"
    ) as f:
        signatures_by_set: Dict[str, Dict[str, str]] = json.load(f)

    if mtgjson_card.set_code not in signatures_by_set:
        return None

    match = re.match("^([^0-9]+)([0-9]+)(.*)", mtgjson_card.number)
    if not match or (match.group(2) == "0" and match.group(3) == "b"):
        return None

    return signatures_by_set[mtgjson_card.set_code].get(match.group(1))


def add_meld_face_parts(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Some sets that like to torture us have cards with multiple cards
    required to summon the grand behemoth. This method creates a
    static card_parts for each card in that sequence for better
    processing. Order will be top card, bottom card, combined card
    ...until Wizards does something else.
    :param mtgjson_set: MTGJSON Set
    """
    LOGGER.info(f"Adding Card Face Parts for {mtgjson_set.code}")

    collector_numbers_in_set = [card.number for card in mtgjson_set.cards]
    for first_card in mtgjson_set.cards:
        if first_card.layout != "meld":
            continue

        card_face_parts: List[Optional[str]] = [None, None, None]

        if f"{first_card.number}b" in collector_numbers_in_set:
            card_face_parts[1] = first_card.face_name
        elif "b" in first_card.number:
            card_face_parts[2] = first_card.face_name
        else:
            card_face_parts[0] = first_card.face_name

        for other_card in mtgjson_set.cards:
            if (
                other_card.layout != "meld"
                or first_card == other_card
                or other_card.face_name not in first_card.get_names()
            ):
                continue

            if f"{other_card.number}b" in collector_numbers_in_set:
                card_face_parts[1] = other_card.face_name
            elif "b" in other_card.number:
                card_face_parts[2] = other_card.face_name
            else:
                card_face_parts[0] = other_card.face_name

        if any(not x for x in card_face_parts):
            LOGGER.warning(
                f"Unable to properly parse Card Parts for {first_card.name} ({first_card.uuid})"
            )
            continue

        first_card.card_parts = [x for x in card_face_parts if x]
    LOGGER.info(f"Finished adding Card Face Parts for {mtgjson_set.code}")


def add_orientations(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Card token orientations are a bit non-standard, so we'll need
    a way to figure this out. This will update in-line
    :param mtgjson_set: MTGJSON Set Object
    """
    LOGGER.info(f"Adding Token Orientations to {mtgjson_set.code}")
    uuid_to_orientation_map = (
        ScryfallProviderOrientationDetector().get_uuid_to_orientation_map(
            mtgjson_set.code
        )
    )
    for token in mtgjson_set.tokens:
        if token.identifiers.scryfall_id:
            token.orientation = uuid_to_orientation_map.get(
                token.identifiers.scryfall_id
            )
    LOGGER.info(f"Finished Token Orientations to {mtgjson_set.code}")


def add_secret_lair_names(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Secret Lairs don't have a native way to know what printing(s) they were in,
    so we will map them to MTG.Wiki to support this functionality
    :param mtgjson_set: MTGJSON Set Object
    """
    LOGGER.info(f"Linking Secret Lair Drops to {mtgjson_set.code}")
    relation_map = MtgWikiProviderSecretLair().download()
    for card in mtgjson_set.cards:
        if card.number in relation_map:
            card.subsets = [relation_map[card.number]]
    LOGGER.info(f"Finished Linking Secret Lair Drops to {mtgjson_set.code}")


def add_related_cards(
    scryfall_object: Dict[str, Any], mtgjson_card: MtgjsonCardObject, is_token: bool
) -> None:
    """Add related card entities to the MTGJSON Card."""
    related_cards = MtgjsonRelatedCardsObject()

    if is_token:
        reverse_related: List[str] = []
        all_parts = scryfall_object.get("all_parts") or []
        for a_part in all_parts:
            if a_part.get("name") != mtgjson_card.name:
                reverse_related.append(a_part.get("name"))
        mtgjson_card.reverse_related = sorted(reverse_related)
        related_cards.reverse_related = sorted(reverse_related)

    set_type = scryfall_object.get("set_type") or ""
    if "alchemy" in set_type:
        alchemy_cards = ScryfallProvider().get_alchemy_cards_with_spellbooks()
        if mtgjson_card.name in alchemy_cards:
            related_cards.spellbook = sorted(
                ScryfallProvider().get_card_names_in_spellbook(mtgjson_card.name)
            )

    if related_cards.present():
        mtgjson_card.related_cards = related_cards


def add_card_products_to_cards(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Add what product(s) each card can be found in, using sealedProduct UUIDs
    :param mtgjson_set MTGJSON Set object to modify in place
    """
    for card_entity in mtgjson_set.cards:
        card_entity.source_products = (
            GitHubCardSealedProductsProvider().get_products_card_found_in(
                card_entity.uuid
            )
        )
    for card_entity in mtgjson_set.tokens:
        card_entity.source_products = (
            GitHubCardSealedProductsProvider().get_products_card_found_in(
                card_entity.uuid
            )
        )


def apply_manual_overrides(mtgjson_cards: List[MtgjsonCardObject]) -> None:
    """
    Sometimes, coding and automation just isn't good enough. In those cases,
    we can apply manual overrides to certain card UUIDs from the manual_overrides list.
    :param mtgjson_cards: MTGJSON Card objects to modify
    """
    with RESOURCE_PATH.joinpath("manual_overrides.json").open(encoding="utf-8") as fp:
        uuid_to_overrides = json.load(fp)

    for mtgjson_card in mtgjson_cards:
        if mtgjson_card.uuid not in uuid_to_overrides:
            continue

        for key, value in uuid_to_overrides[mtgjson_card.uuid].items():
            if key.startswith("__"):
                continue
            setattr(mtgjson_card, key, value)
