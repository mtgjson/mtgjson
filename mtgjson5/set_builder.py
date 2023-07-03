"""
MTGJSON Set Builder
"""
import json
import logging
import pathlib
import re
import unicodedata
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from . import constants
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
)
from .providers import (
    CardKingdomProvider,
    CardMarketProvider,
    CardMarketProviderSetNameTranslations,
    EdhrecProviderCardRanks,
    FandomProviderSecretLair,
    GathererProvider,
    GitHubBoostersProvider,
    GitHubDecksProvider,
    GitHubSealedProvider,
    MTGBanProvider,
    MultiverseBridgeProvider,
    ScryfallProvider,
    ScryfallProviderOrientationDetector,
    ScryfallProviderSetLanguageDetector,
    TCGPlayerProvider,
    WhatsInStandardProvider,
)
from .utils import get_str_or_none, parallel_call, url_keygen

LOGGER = logging.getLogger(__name__)


def parse_foreign(
    sf_prints_url: str, card_name: str, card_number: str, set_name: str
) -> List[MtgjsonForeignDataObject]:
    """
    Get the foreign printings information for a specific card
    :param card_number: Card's number
    :param sf_prints_url: URL to get prints from
    :param card_name: Card name to parse (needed for double faced)
    :param set_name: Set name
    :return: Foreign entries object
    """
    card_foreign_entries: List[MtgjsonForeignDataObject] = []

    # Add information to get all languages
    sf_prints_url = sf_prints_url.replace("&unique=prints", "+lang%3Aany&unique=prints")

    prints_api_json = ScryfallProvider().download_all_pages(sf_prints_url)
    if not prints_api_json:
        LOGGER.error(f"No data found for {sf_prints_url}: {prints_api_json}")
        return []

    for foreign_card in prints_api_json:
        if (
            set_name != foreign_card["set"]
            or card_number != foreign_card["collector_number"]
            or foreign_card["lang"] == "en"
        ):
            continue

        card_foreign_entry = MtgjsonForeignDataObject()
        try:
            card_foreign_entry.language = constants.LANGUAGE_MAP[foreign_card["lang"]]
        except IndexError:
            LOGGER.warning(f"Unable to get language {foreign_card}")

        if foreign_card["multiverse_ids"]:
            card_foreign_entry.multiverse_id = foreign_card["multiverse_ids"][0]

        if "card_faces" in foreign_card:
            if card_name.lower() == foreign_card["name"].split("/")[0].strip().lower():
                face = 0
            else:
                face = 1

            LOGGER.debug(f"Split card found: Using face {face} for {card_name}")
            card_foreign_entry.name = " // ".join(
                [
                    face_data.get("printed_name", face_data.get("name", ""))
                    for face_data in foreign_card["card_faces"]
                ]
            )

            foreign_card = foreign_card["card_faces"][face]
            card_foreign_entry.face_name = foreign_card.get("printed_name")
            if not card_foreign_entry.face_name:
                LOGGER.warning(f"Unable to resolve name for {foreign_card}")
                card_foreign_entry.face_name = foreign_card.get("name")

        if not card_foreign_entry.name:
            card_foreign_entry.name = foreign_card.get("printed_name")

            # https://github.com/mtgjson/mtgjson/issues/611
            if set_name.upper() == "IKO" and card_foreign_entry.language == "Japanese":
                card_foreign_entry.name = str(card_foreign_entry.name).split(
                    " //", maxsplit=1
                )[0]

        card_foreign_entry.text = foreign_card.get("printed_text")
        card_foreign_entry.flavor_text = foreign_card.get("flavor_text")
        card_foreign_entry.type = foreign_card.get("printed_type_line")

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
        # are split by spaces
        if card_type.startswith("Plane"):
            sub_types = [subtypes.strip()]
        else:
            sub_types = [x.strip() for x in subtypes.split() if x]

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
    Get a Scryfall set header for a specific set
    :param set_code: Set to grab header for
    :return: Set header, if it exists
    """
    set_data: Dict[str, Any] = ScryfallProvider().download(
        ScryfallProvider().ALL_SETS_URL + set_code
    )

    if set_data["object"] == "error":
        LOGGER.error(f"Failed to download {set_code}")
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


def parse_printings(sf_prints_url: Optional[str]) -> List[str]:
    """
    Given a Scryfall printings URL, extract all sets a card was printed in
    :param sf_prints_url: URL to extract data from
    :return: List of all sets a specific card was printed in
    """
    card_sets: Set[str] = set()

    while sf_prints_url:
        prints_api_json: Dict[str, Any] = ScryfallProvider().download(sf_prints_url)

        if prints_api_json["object"] == "error":
            LOGGER.error(f"Bad download: {sf_prints_url}")
            break

        for card in prints_api_json["data"]:
            card_sets.add(card.get("set").upper())

        if not prints_api_json.get("has_more"):
            break

        sf_prints_url = prints_api_json.get("next_page")

    return sorted(list(card_sets))


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


def parse_rulings(rulings_url: str) -> List[MtgjsonRulingObject]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = ScryfallProvider().download(rulings_url)
    if rules_api_json["object"] == "error":
        LOGGER.error(f"Error downloading URL {rulings_url}: {rules_api_json}")
        return []

    mtgjson_rules: List[MtgjsonRulingObject] = []

    for sf_rule in rules_api_json["data"]:
        mtgjson_rule = MtgjsonRulingObject(sf_rule["published_at"], sf_rule["comment"])
        mtgjson_rules.append(mtgjson_rule)

    return sorted(mtgjson_rules, key=lambda ruling: ruling.date)


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
    remove them from the cards array and sets an internal market
    to be dealt with later down the line
    :param mtgjson_set: MTGJSON Set object
    """
    LOGGER.info(f"Relocate tokens for {mtgjson_set.code}")
    token_types = {"token", "double_faced_token", "emblem", "art_series"}

    # Identify unique tokens from cards
    tokens_found = {
        card.identifiers.scryfall_id
        for card in mtgjson_set.cards
        if card.layout in token_types and card.identifiers.scryfall_id
    }

    # Remove tokens from cards
    mtgjson_set.cards[:] = (
        card for card in mtgjson_set.cards if card.layout not in token_types
    )

    # Scryfall objects to handle later
    mtgjson_set.extra_tokens = [
        ScryfallProvider().download(ScryfallProvider().CARDS_URL + scryfall_id)
        for scryfall_id in tokens_found
    ]
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


def build_mtgjson_set(set_code: str) -> Optional[MtgjsonSetObject]:
    """
    Construct a MTGJSON Magic Set
    :param set_code: Set to construct
    :return: Set object
    """
    # Output Object
    mtgjson_set = MtgjsonSetObject()

    # Ensure we have a header for this set
    set_data = get_scryfall_set_data(set_code)
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
    mtgjson_set.languages = (
        ScryfallProviderSetLanguageDetector().get_set_printing_languages(
            mtgjson_set.code
        )
    )
    mtgjson_set.mcm_name = CardMarketProvider().get_set_name(mtgjson_set.name)
    mtgjson_set.mcm_id = CardMarketProvider().get_set_id(mtgjson_set.name)
    mtgjson_set.mcm_id_extras = CardMarketProvider().get_extras_set_id(mtgjson_set.name)
    mtgjson_set.translations = (
        CardMarketProviderSetNameTranslations().get_set_translation_object(
            mtgjson_set.code
        )
    )

    # Building cards is a process
    mtgjson_set.cards = build_base_mtgjson_cards(
        set_code, set_release_date=mtgjson_set.release_date
    )
    add_is_starter_option(set_code, mtgjson_set.search_uri, mtgjson_set.cards)
    add_rebalanced_to_original_linkage(mtgjson_set)
    relocate_miscellaneous_tokens(mtgjson_set)

    if not any(card.identifiers.multiverse_id for card in mtgjson_set.cards):
        add_slow_gatherer_multiverse_ids_if_necessary(mtgjson_set)

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

    mtgjson_set.tcgplayer_group_id = set_data.get("tcgplayer_id")
    mtgjson_set.booster = GitHubBoostersProvider().get_set_booster_data(set_code)

    # Build sealed product using the TCGPlayer data
    mtgjson_set.sealed_product = (
        TCGPlayerProvider().generate_mtgjson_sealed_product_objects(
            mtgjson_set.tcgplayer_group_id, mtgjson_set.code
        )
    )
    CardKingdomProvider().update_sealed_product(
        mtgjson_set.name, mtgjson_set.sealed_product
    )
    sealed_provider = GitHubSealedProvider()
    mtgjson_set.sealed_product.extend(
        sealed_provider.get_sealed_products_data(set_code)
    )
    sealed_provider.apply_sealed_contents_data(set_code, mtgjson_set)
    add_sealed_uuid(mtgjson_set)
    add_sealed_purchase_url(mtgjson_set)
    add_token_signatures(mtgjson_set)

    add_multiverse_bridge_ids(mtgjson_set)

    mark_duel_decks(set_code, mtgjson_set.cards)

    mtgjson_set.decks = GitHubDecksProvider().get_decks_in_set(set_code)
    for mtgjson_set_deck in mtgjson_set.decks:
        mtgjson_set_deck.add_sealed_product_uuids(mtgjson_set.sealed_product)

    if "Art Series" in mtgjson_set.name:
        add_orientations(mtgjson_set)

    # Implicit Variables
    mtgjson_set.is_foreign_only = mtgjson_set.code in constants.FOREIGN_SETS
    mtgjson_set.is_partial_preview = MtgjsonMetaObject().date < mtgjson_set.release_date

    return mtgjson_set


def add_slow_gatherer_multiverse_ids_if_necessary(
    mtgjson_set: MtgjsonSetObject,
) -> None:
    """
    If our upstream providers are lacking Multiverse IDs, we can manually pull them
    from Gatherer ourselves, albeit a relatively slow operation.
    :param mtgjson_set: the set to add multiverse ids to
    """
    LOGGER.info(f"Attempting to add Multiverse IDs to {mtgjson_set.code}")
    card_number_to_multiverse_ids = (
        GathererProvider().get_collector_number_to_multiverse_id_mapping(
            mtgjson_set.name
        )
    )
    for card in mtgjson_set.cards:
        LOGGER.info(
            f"Adding backup Multiverse ID {card_number_to_multiverse_ids.get(card.number)} to {card.name}"
        )
        card.identifiers.multiverse_id = card_number_to_multiverse_ids.get(card.number)


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


def add_sealed_uuid(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Adds all uuids to each sealed product object within a set
    :param mtgjson_set: the set to add sealed uuids to
    """
    for sealed_product in mtgjson_set.sealed_product:
        add_uuid(sealed_product)


def add_sealed_purchase_url(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Adds all purchase urls to each sealed product object within a set
    :param mtgjson_set: the set to add purchase urls to
    """
    for sealed_product in mtgjson_set.sealed_product:
        if (
            hasattr(sealed_product.identifiers, "tcgplayer_product_id")
            and sealed_product.identifiers.tcgplayer_product_id
        ):
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
    cards = ScryfallProvider().download_cards(set_code)
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
    set_code: str, search_url: str, mtgjson_cards: List[MtgjsonCardObject]
) -> None:
    """
    There are cards that may not exist in standard boosters. As such, we mark
    those as starter cards.
    :param set_code: Set to handle
    :param search_url: URL to search for cards in
    :param mtgjson_cards: Card Objects to modify
    """
    LOGGER.info(f"Add starter data to {set_code}")
    starter_card_url = search_url.replace("&unique=", "++not:booster&unique=")
    starter_cards = ScryfallProvider().download(starter_card_url)

    if starter_cards["object"] == "error":
        LOGGER.debug(f"All cards in {set_code} are available in boosters")
        LOGGER.info(f"Finished adding starter data to {set_code}")
        return

    for scryfall_object in starter_cards["data"]:
        mtgjson_cards_with_same_id = [
            item
            for item in mtgjson_cards
            if item.identifiers.scryfall_id == scryfall_object["id"]
        ]

        for card in mtgjson_cards_with_same_id:
            card.is_starter = True
    LOGGER.info(f"Finished adding starter data to {set_code}")


def add_leadership_skills(mtgjson_card: MtgjsonCardObject) -> None:
    """
    Determine if a card is able to be your commander, and if so
    which format(s).
    :param mtgjson_card: Card object
    """
    # These are cards that can be your commander, even if not intuitive
    override_cards = ("Grist, the Hunger Tide",)

    is_commander_legal = (
        mtgjson_card.name in override_cards
        or (
            "Legendary" in mtgjson_card.type
            and "Creature" in mtgjson_card.type
            # Exclude Flip cards
            and mtgjson_card.type not in {"flip"}
            # Exclude Melded cards and backside of Transform cards
            and (mtgjson_card.side == "a" if mtgjson_card.side else True)
        )
        or ("can be your commander" in mtgjson_card.text)
    )

    is_oathbreaker_legal = "Planeswalker" in mtgjson_card.type

    is_brawl_legal = (
        mtgjson_card.set_code.upper() in WhatsInStandardProvider().set_codes
        and (is_oathbreaker_legal or is_commander_legal)
    )

    if is_commander_legal or is_oathbreaker_legal or is_brawl_legal:
        mtgjson_card.leadership_skills = MtgjsonLeadershipSkillsObject(
            is_brawl_legal, is_commander_legal, is_oathbreaker_legal
        )


def add_uuid(
    mtgjson_object: Union[MtgjsonCardObject, MtgjsonSealedProductObject]
) -> None:
    """
    Construct a UUIDv5 for each MTGJSON card object
    This will also add UUIDv4 for legacy support
    :param mtgjson_object: Card object
    """
    if isinstance(mtgjson_object, MtgjsonSealedProductObject):
        mtgjson_object.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, mtgjson_object.name))
    else:
        if {"Token", "Card"}.intersection(mtgjson_object.types):
            # Tokens have a special generation method
            id_source_v5 = (
                mtgjson_object.name
                + (mtgjson_object.face_name or "")
                + "".join((mtgjson_object.colors or ""))
                + (mtgjson_object.power or "")
                + (mtgjson_object.toughness or "")
                + (mtgjson_object.side or "")
                + mtgjson_object.set_code[1:].lower()
                + (mtgjson_object.identifiers.scryfall_id or "")
                + (mtgjson_object.identifiers.scryfall_illustration_id or "")
            )

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
            id_source_v5 = (
                ScryfallProvider().get_class_id()
                + (mtgjson_object.identifiers.scryfall_id or "")
                + (mtgjson_object.identifiers.scryfall_illustration_id or "")
                + mtgjson_object.set_code.lower()
                + mtgjson_object.name
                + (mtgjson_object.face_name or "")
            )

            id_source_v4 = (
                "sf"
                + (mtgjson_object.identifiers.scryfall_id or "")
                + (
                    mtgjson_object.face_name
                    if mtgjson_object.face_name
                    else mtgjson_object.name
                )
            )

        mtgjson_object.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v5))
        mtgjson_object.identifiers.mtgjson_v4_id = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, id_source_v4)
        )


def build_mtgjson_card(
    scryfall_object: Dict[str, Any],
    face_id: int = 0,
    is_token: bool = False,
    set_release_date: str = "",
) -> List[MtgjsonCardObject]:
    """
    Construct a MTGJSON Card object from 3rd party
    entities
    :param scryfall_object: Scryfall Card Object
    :param face_id: What face to build for (set internally)
    :param is_token: Is this a token object? (some diff fields)
    :param set_release_date: Original set release date
    :return: List of card objects that were constructed
    """
    LOGGER.info(f"Building {scryfall_object['set'].upper()}: {scryfall_object['name']}")

    # Return List
    mtgjson_cards = []

    # Object Container
    mtgjson_card = MtgjsonCardObject(is_token)

    mtgjson_card.name = scryfall_object["name"]
    mtgjson_card.language = constants.LANGUAGE_MAP[scryfall_object["lang"]]
    mtgjson_card.flavor_name = scryfall_object.get("flavor_name")
    mtgjson_card.set_code = scryfall_object["set"].upper()
    mtgjson_card.identifiers.scryfall_id = scryfall_object["id"]
    mtgjson_card.identifiers.scryfall_oracle_id = (
        scryfall_object.get("oracle_id")
    ) or scryfall_object["card_faces"][face_id].get("oracle_id")

    # Handle atypical cards
    face_data = scryfall_object
    if "card_faces" in scryfall_object:
        mtgjson_card.set_names(scryfall_object["name"].split("//"))
        mtgjson_card.set_illustration_ids(
            [
                card_face.get("illustration_id", "Missing")
                for card_face in scryfall_object["card_faces"]
            ]
        )

        # Override face_data from above
        face_data = scryfall_object["card_faces"][face_id]

        if face_data.get("flavor_name"):
            mtgjson_card.flavor_name = " // ".join(
                [entry["flavor_name"] for entry in scryfall_object["card_faces"]]
            )
            mtgjson_card.face_flavor_name = face_data["flavor_name"]

        if "//" in scryfall_object.get("mana_cost", ""):
            mtgjson_card.colors = get_card_colors(
                scryfall_object["mana_cost"].split("//")[face_id]
            )
            mtgjson_card.face_mana_value = get_card_cmc(
                scryfall_object["mana_cost"].split("//")[face_id]
            )
            # Deprecated - Remove in 6.0.0
            mtgjson_card.face_converted_mana_cost = get_card_cmc(
                scryfall_object["mana_cost"].split("//")[face_id]
            )
        elif scryfall_object["layout"] in {
            "split",
            "transform",
            "aftermath",
            "adventure",
        }:
            mtgjson_card.face_mana_value = get_card_cmc(face_data.get("mana_cost", "0"))
            # Deprecated - Remove in 6.0.0
            mtgjson_card.face_converted_mana_cost = mtgjson_card.face_mana_value

            # Modal DFCs have their face & normal mana cost the same
        elif scryfall_object["layout"] == "modal_dfc":
            mtgjson_card.mana_value = get_card_cmc(face_data.get("mana_cost", "0"))
            # Deprecated - Remove in 6.0.0
            mtgjson_card.converted_mana_cost = mtgjson_card.mana_value
        elif scryfall_object["layout"] == "reversible_card":
            mtgjson_card.mana_value = face_data.get("cmc", 0)
            mtgjson_card.converted_mana_cost = mtgjson_card.mana_value

        mtgjson_card.set_watermark(scryfall_object["card_faces"][0].get("watermark"))

        if scryfall_object["card_faces"][-1]["oracle_text"].startswith("Aftermath"):
            mtgjson_card.layout = "aftermath"

        mtgjson_card.artist = scryfall_object["card_faces"][face_id].get("artist", "")
        mtgjson_card.artist_ids = scryfall_object["card_faces"][face_id].get(
            "artist_ids", ""
        )

        if face_id == 0:
            for i in range(1, len(scryfall_object["card_faces"])):
                mtgjson_cards.extend(
                    build_mtgjson_card(scryfall_object, i, is_token, set_release_date)
                )

    # Start of single card builder
    if face_data.get("mana_cost"):
        mtgjson_card.mana_cost = face_data["mana_cost"]

    mtgjson_card.identifiers.scryfall_illustration_id = scryfall_object.get(
        "illustration_id", face_data.get("illustration_id")
    )

    if not mtgjson_card.colors:
        mtgjson_card.colors = (
            face_data["colors"]
            if "colors" in face_data.keys()
            else scryfall_object.get("colors", [])
        )

    # Explicit Variables -- Based on the entire card object

    mtgjson_card.attraction_lights = scryfall_object.get("attraction_lights")
    mtgjson_card.border_color = scryfall_object.get("border_color", "")
    mtgjson_card.color_identity = scryfall_object.get("color_identity", "")
    if not hasattr(mtgjson_card, "mana_value"):
        mtgjson_card.mana_value = scryfall_object.get("cmc", "")
        # Deprecated - Remove in 6.0.0
        mtgjson_card.converted_mana_cost = scryfall_object.get("cmc", "")
    mtgjson_card.edhrec_rank = scryfall_object.get("edhrec_rank")
    mtgjson_card.edhrec_saltiness = EdhrecProviderCardRanks().get_salt_rating(
        mtgjson_card.name
    )
    mtgjson_card.finishes = scryfall_object.get("finishes", [])
    mtgjson_card.frame_effects = scryfall_object.get("frame_effects", "")
    mtgjson_card.frame_version = scryfall_object.get("frame", "")
    mtgjson_card.hand = scryfall_object.get("hand_modifier")
    mtgjson_card.has_foil = any(
        finish in scryfall_object.get("finishes", []) for finish in ("foil", "glossy")
    )
    mtgjson_card.has_non_foil = "nonfoil" in scryfall_object.get("finishes", [])
    mtgjson_card.has_content_warning = scryfall_object.get("content_warning")
    mtgjson_card.is_full_art = scryfall_object.get("full_art")
    mtgjson_card.is_online_only = scryfall_object.get("digital")
    mtgjson_card.is_oversized = scryfall_object.get("oversized") or (
        mtgjson_card.set_code in ("OC21",)
    )
    mtgjson_card.is_promo = scryfall_object.get("promo")
    mtgjson_card.is_reprint = scryfall_object.get("reprint")
    mtgjson_card.is_reserved = scryfall_object.get("reserved")
    mtgjson_card.is_story_spotlight = scryfall_object.get("story_spotlight")
    mtgjson_card.is_textless = scryfall_object.get("textless")
    mtgjson_card.life = scryfall_object.get("life_modifier")

    # Future expansion to support set and collector booster types
    mtgjson_card.booster_types = []
    if scryfall_object.get("booster", False):
        mtgjson_card.booster_types.append("default")
    if any(
        deck_type in scryfall_object.get("promo_types", [])
        for deck_type in ("starterdeck", "planeswalkerdeck")
    ):
        mtgjson_card.booster_types.append("deck")

    mtgjson_card.identifiers.mcm_id = get_str_or_none(
        scryfall_object.get("cardmarket_id")
    )
    mtgjson_card.identifiers.mtg_arena_id = get_str_or_none(
        scryfall_object.get("arena_id")
    )
    mtgjson_card.identifiers.mtgo_id = get_str_or_none(scryfall_object.get("mtgo_id"))
    mtgjson_card.identifiers.mtgo_foil_id = get_str_or_none(
        scryfall_object.get("mtgo_foil_id")
    )
    mtgjson_card.number = scryfall_object.get("collector_number", "0")
    mtgjson_card.security_stamp = scryfall_object.get("security_stamp")

    # Handle Promo Types for MTGJSON
    mtgjson_card.promo_types = scryfall_object.get("promo_types", [])
    if mtgjson_card.number.endswith("p"):
        mtgjson_card.promo_types.append("planeswalkerstamped")

    # Remove terms that are covered elsewhere
    mtgjson_card.promo_types = [
        card_type
        for card_type in mtgjson_card.promo_types
        if card_type not in {"starterdeck", "planeswalkerdeck"}
    ]

    card_release_date = scryfall_object.get("released_at")
    if set_release_date and set_release_date != card_release_date:
        mtgjson_card.original_release_date = card_release_date

    mtgjson_card.rarity = scryfall_object.get("rarity", "")
    if not mtgjson_card.artist:
        mtgjson_card.artist = scryfall_object.get("artist", "")
    if not mtgjson_card.artist_ids:
        mtgjson_card.artist_ids = scryfall_object.get("artist_ids", "")
    if not mtgjson_card.watermark:
        mtgjson_card.set_watermark(face_data.get("watermark"))

    if scryfall_object.get("layout") == "art_series":
        mtgjson_card.layout = "art_series"
    elif "//" not in mtgjson_card.name and any(
        type_line in scryfall_object.get("type_line", "").lower()
        for type_line in ("card", "token")
    ):
        # Cards are just tokens in disguise!
        mtgjson_card.layout = "token"

    if not mtgjson_card.layout:
        mtgjson_card.layout = scryfall_object.get("layout", "")

    # Indicate if this component exists on the platform
    mtgjson_card.availability = MtgjsonGameFormatsObject()
    mtgjson_card.availability.arena = "arena" in scryfall_object.get("games", []) or (
        mtgjson_card.identifiers.mtg_arena_id is not None
    )
    mtgjson_card.availability.mtgo = "mtgo" in scryfall_object.get("games", []) or (
        mtgjson_card.identifiers.mtgo_id is not None
    )
    mtgjson_card.availability.paper = not mtgjson_card.is_online_only
    mtgjson_card.availability.shandalar = "astral" in scryfall_object.get("games", [])
    mtgjson_card.availability.dreamcast = "sega" in scryfall_object.get("games", [])

    # Explicit Variables -- Based on the face of the card
    mtgjson_card.loyalty = face_data.get("loyalty")
    mtgjson_card.defense = face_data.get("defense")

    ascii_name = (
        unicodedata.normalize("NFD", mtgjson_card.name)
        .encode("ascii", "ignore")
        .decode()
    )
    if mtgjson_card.name != ascii_name:
        LOGGER.debug(f"Adding ascii name for {mtgjson_card.name} -> {ascii_name}")
        mtgjson_card.ascii_name = ascii_name

    mtgjson_card.power = face_data.get("power", "")
    mtgjson_card.text = face_data.get("oracle_text", "")
    mtgjson_card.toughness = face_data.get("toughness", "")
    mtgjson_card.type = face_data.get("type_line", "Card")

    # Explicit -- Depending on if card face has it or not
    mtgjson_card.flavor_text = (
        face_data.get("flavor_text")
        if face_data.get("flavor_text")
        else scryfall_object.get("flavor_text")
    )

    if "color_indicator" in face_data.keys():
        mtgjson_card.color_indicator = face_data["color_indicator"]
    elif "color_indicator" in scryfall_object.keys():
        mtgjson_card.color_indicator = scryfall_object["color_indicator"]

    if scryfall_object["multiverse_ids"]:
        if len(scryfall_object["multiverse_ids"]) > face_id:
            mtgjson_card.identifiers.multiverse_id = get_str_or_none(
                scryfall_object["multiverse_ids"][face_id]
            )
        else:
            mtgjson_card.identifiers.multiverse_id = get_str_or_none(
                scryfall_object["multiverse_ids"][0]
            )

    # Add "side" for split cards (cards with exactly 2 sides)
    # Also set face name
    face_names = mtgjson_card.get_names()
    if face_names:
        mtgjson_card.face_name = str(face_data["name"])

        if mtgjson_card.layout != "meld":
            # Fix #632 as there are very limited distinguishing attributes
            if mtgjson_card.set_code.lower() == "tust":
                mtgjson_card.side = "a" if mtgjson_card.type != "Token" else "b"
            elif face_names.count(face_names[0]) == len(face_names):
                # Art Series have a unique way of determining the side
                face_illustration_ids = mtgjson_card.get_illustration_ids()

                # Some tokens have the same IDs on both sides in AAFR, for example
                if len(set(face_illustration_ids)) == 1:
                    mtgjson_card.side = chr(face_id + 97)
                else:
                    for index in range(len(face_names)):
                        if (
                            face_illustration_ids[index]
                            == mtgjson_card.identifiers.scryfall_illustration_id
                        ):
                            # chr(97) = 'a', chr(98) = 'b', ...
                            mtgjson_card.side = chr(index + 97)
                            break

                if (
                    mtgjson_card.identifiers.scryfall_illustration_id is None
                    and "Missing" in face_illustration_ids
                ):
                    mtgjson_card.side = chr(face_id + 97)
            else:
                # Standard flip cards and such
                # chr(97) = 'a', chr(98) = 'b', ...
                mtgjson_card.side = chr(face_names.index(mtgjson_card.face_name) + 97)

    # Implicit Variables
    mtgjson_card.is_funny = scryfall_object.get("set_type") in {"funny"} and (
        mtgjson_card.security_stamp in {"acorn"}
        if mtgjson_card.set_code in {"UNF"}
        else True
    )
    mtgjson_card.is_timeshifted = (
        scryfall_object.get("frame") == "future"
        or mtgjson_card.set_code.lower() == "tsb"
    )
    mtgjson_card.printings = parse_printings(
        scryfall_object["prints_search_uri"].replace("%22", "")
    )
    mtgjson_card.legalities = parse_legalities(
        scryfall_object["legalities"]
        if scryfall_object.get("set_type") not in ["memorabilia"]
        else {}
    )
    mtgjson_card.rulings = parse_rulings(scryfall_object["rulings_uri"])

    card_types = parse_card_types(mtgjson_card.type)
    mtgjson_card.supertypes = card_types[0]
    mtgjson_card.types = card_types[1]
    mtgjson_card.subtypes = card_types[2]

    if mtgjson_card.name.startswith("A-"):
        mtgjson_card.is_alternative = True
        mtgjson_card.is_rebalanced = True

    if "Planeswalker" in mtgjson_card.types:
        mtgjson_card.text = re.sub(r"([+−-]?[0-9X]+):", r"[\1]:", mtgjson_card.text)

    # Keywords have to be split up on our end for individual card faces
    mtgjson_card.keywords = [
        keyword
        for keyword in sorted(scryfall_object.get("keywords", []))
        if keyword.lower() in mtgjson_card.text.lower()
    ]

    # Handle Meld components, as well as tokens
    if "all_parts" in scryfall_object.keys():
        meld_object = []
        mtgjson_card.set_names(None)
        for a_part in sorted(
            scryfall_object["all_parts"], key=lambda part: part["component"]  # type: ignore
        ):
            if a_part["component"] == "token":
                continue

            # This is a meld only-fix, so we ignore tokens/combo pieces
            if a_part["component"].startswith("meld"):
                meld_object.append(a_part["component"])
                mtgjson_card.append_names(a_part.get("name"))
                continue

            # There are a handful of cards that have multiple incorrect listings
            # in their parts, such as rebalanced (Alrund) and same card double flipped (Zndrsplt)
            if mtgjson_card.name in a_part.get("name") and "//" in a_part.get("name"):
                mtgjson_card.set_names(a_part.get("name").split("//"))
                break

        # If the only entry is the original card, empty the names array
        if (
            mtgjson_card.get_names()
            and len(mtgjson_card.get_names()) == 1
            and mtgjson_card.name in mtgjson_card.get_names()
        ):
            mtgjson_card.set_names(None)

        # Meld Object; get_names() => CardA, CardB, Meld
        if mtgjson_card.get_names() and len(mtgjson_card.get_names()) == 3:
            # Front Sides will have name = Front1//Back, Front2//Back
            # Back Side will have name = Back
            if mtgjson_card.name != mtgjson_card.get_names()[2]:
                mtgjson_card.side = "a"
                mtgjson_card.face_name = mtgjson_card.name
                mtgjson_card.name = (
                    f"{mtgjson_card.name} // {mtgjson_card.get_names()[2]}"
                )
            else:
                mtgjson_card.face_name = mtgjson_card.name
                mtgjson_card.side = "b"

    mtgjson_card.foreign_data = parse_foreign(
        scryfall_object["prints_search_uri"].replace("%22", ""),
        mtgjson_card.face_name if mtgjson_card.face_name else mtgjson_card.name,
        mtgjson_card.number,
        mtgjson_card.set_code.lower(),
    )

    if mtgjson_card.name in ScryfallProvider().cards_without_limits:
        mtgjson_card.has_alternative_deck_limit = True

    add_uuid(mtgjson_card)
    add_leadership_skills(mtgjson_card)

    # Add purchase URL components after UUIDs are finalized
    mtgjson_card.raw_purchase_urls.update(scryfall_object.get("purchase_uris", {}))
    if "tcgplayer_id" in scryfall_object:
        mtgjson_card.identifiers.tcgplayer_product_id = str(
            scryfall_object["tcgplayer_id"]
        )
        mtgjson_card.purchase_urls.tcgplayer = url_keygen(
            mtgjson_card.identifiers.tcgplayer_product_id + mtgjson_card.uuid
        )
    if "tcgplayer_etched_id" in scryfall_object:
        mtgjson_card.identifiers.tcgplayer_etched_product_id = str(
            scryfall_object["tcgplayer_etched_id"]
        )
        mtgjson_card.purchase_urls.tcgplayer_etched = url_keygen(
            mtgjson_card.identifiers.tcgplayer_etched_product_id + mtgjson_card.uuid
        )
        # Have to manually insert
        mtgjson_card.raw_purchase_urls["tcgplayerEtched"] = (
            "https://shop.tcgplayer.com/product/productsearch"
            + f"?id={mtgjson_card.identifiers.tcgplayer_etched_product_id}"
            + "&utm_campaign=affiliate&utm_medium=api&utm_source=mtgjson"
        )

    add_related_cards(scryfall_object, mtgjson_card, is_token)

    # Gatherer Calls -- SLOWWWWW
    if mtgjson_card.identifiers.multiverse_id:
        gatherer_cards = GathererProvider().get_cards(
            mtgjson_card.identifiers.multiverse_id, mtgjson_card.set_code.lower()
        )
        if len(gatherer_cards) > face_id:
            mtgjson_card.original_type = gatherer_cards[face_id].original_types
            mtgjson_card.original_text = gatherer_cards[face_id].original_text

    mtgjson_cards.append(mtgjson_card)

    return mtgjson_cards


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
    translation_table = MTGBanProvider().get_mtgjson_to_card_kingdom()

    for mtgjson_card in mtgjson_set.cards + mtgjson_set.tokens:
        if mtgjson_card.uuid not in translation_table:
            continue

        entry = translation_table[mtgjson_card.uuid]

        if "normal" in entry:
            mtgjson_card.identifiers.card_kingdom_id = str(entry["normal"]["id"])
            mtgjson_card.purchase_urls.card_kingdom = url_keygen(
                entry["normal"]["url"] + mtgjson_card.uuid
            )
            mtgjson_card.raw_purchase_urls.update(
                {
                    "cardKingdom": entry["normal"]["url"]
                    + constants.CARD_KINGDOM_REFERRAL
                }
            )

        if "foil" in entry:
            mtgjson_card.identifiers.card_kingdom_foil_id = str(entry["foil"]["id"])
            mtgjson_card.purchase_urls.card_kingdom_foil = url_keygen(
                entry["foil"]["url"] + mtgjson_card.uuid
            )
            mtgjson_card.raw_purchase_urls.update(
                {
                    "cardKingdomFoil": entry["foil"]["url"]
                    + constants.CARD_KINGDOM_REFERRAL
                }
            )

        if "etched" in entry:
            mtgjson_card.identifiers.card_kingdom_etched_id = str(entry["etched"]["id"])
            mtgjson_card.purchase_urls.card_kingdom_etched = url_keygen(
                entry["etched"]["url"] + mtgjson_card.uuid
            )
            mtgjson_card.raw_purchase_urls.update(
                {
                    "cardKingdomEtched": entry["etched"]["url"]
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
    rosetta_stone_cards = MultiverseBridgeProvider().get_rosetta_stone_cards()
    for mtgjson_card in mtgjson_set.cards:
        if mtgjson_card.identifiers.scryfall_id not in rosetta_stone_cards:
            LOGGER.warning(
                f"MultiverseBridge missing {mtgjson_card.name} in {mtgjson_card.set_code}"
            )
            continue
        mtgjson_card.identifiers.cardsphere_id = str(
            rosetta_stone_cards[mtgjson_card.identifiers.scryfall_id]["cs_id"]
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

    extras_cards: Dict[str, Dict[str, Any]] = {}
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

        mkm_obj = search_cards[card_key]
        if delete_key:
            del search_cards[card_key]

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
    Get the size of a set from scryfall or corrections file
    :param mtgjson_set: Mtgjson Set Object
    :return: Amount of cards in set (base, total)
    """
    # Load cache if not loaded
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
            # Download on the fly
            base_set_size_download = ScryfallProvider().download(
                ScryfallProvider().CARDS_IN_BASE_SET_URL.format(
                    mtgjson_set.code.upper()
                )
            )

            # Wasn't able to determine, so use all cards instead
            if base_set_size_download["object"] == "error":
                base_set_size_download = ScryfallProvider().download(
                    ScryfallProvider().CARDS_IN_SET.format(mtgjson_set.code.upper())
                )

            base_set_size = int(base_set_size_download.get("total_cards", 0))

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
    for first_card in mtgjson_set.cards:
        if first_card.layout != "meld":
            continue

        card_face_parts: List[Optional[str]] = [None, None, None]

        if "a" in first_card.number:
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

            if "a" in other_card.number:
                card_face_parts[1] = other_card.face_name
            elif "b" in other_card.number:
                card_face_parts[2] = other_card.face_name
            else:
                card_face_parts[0] = other_card.face_name

        if any(not x for x in card_face_parts):
            LOGGER.warning(f"Unable to properly parse Card Parts for {first_card}")
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
    so we will map them to the Fandom wiki to support this functionality
    :param mtgjson_set: MTGJSON Set Object
    """
    LOGGER.info(f"Linking Secret Lair Drops to {mtgjson_set.code}")
    relation_map = FandomProviderSecretLair().download()
    for card in mtgjson_set.cards:
        if card.number in relation_map:
            card.subsets = [relation_map[card.number]]
    LOGGER.info(f"Finished Linking Secret Lair Drops to {mtgjson_set.code}")


def add_related_cards(
    scryfall_object: Dict[str, Any], mtgjson_card: MtgjsonCardObject, is_token: bool
) -> None:
    """
    Add related card entities to the MTGJSON Card
    :param scryfall_object: Scryfall data object
    :param mtgjson_card: MTGJSON Card object to modify
    :param is_token: Is the MTGJSON Card a token or not
    """
    related_cards = MtgjsonRelatedCardsObject()

    if is_token:
        reverse_related: List[str] = []
        if "all_parts" in scryfall_object:
            for a_part in scryfall_object["all_parts"]:
                if a_part.get("name") != mtgjson_card.name:
                    reverse_related.append(a_part.get("name"))
        mtgjson_card.reverse_related = reverse_related
        related_cards.reverse_related = reverse_related

    if "alchemy" in scryfall_object["set_type"]:
        alchemy_cards = ScryfallProvider().get_alchemy_cards_with_spellbooks()
        if mtgjson_card.name in alchemy_cards:
            related_cards.spellbook = ScryfallProvider().get_card_names_in_spellbook(
                mtgjson_card.name
            )

    if related_cards.present():
        mtgjson_card.related_cards = related_cards
