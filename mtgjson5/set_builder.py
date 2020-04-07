"""
MTGJSON Set Builder
"""
import json
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional, Set, Tuple
import unicodedata
import uuid

from .classes import (
    MtgjsonCardObject,
    MtgjsonForeignDataObject,
    MtgjsonLeadershipSkillsObject,
    MtgjsonLegalitiesObject,
    MtgjsonMetaObject,
    MtgjsonRulingObject,
    MtgjsonSetObject,
)
from .consts import (
    BASIC_LAND_NAMES,
    CARD_MARKET_BUFFER,
    FOREIGN_SETS,
    LANGUAGE_MAP,
    RESOURCE_PATH,
    SUPER_TYPES,
)
from .providers import (
    GathererProvider,
    McmProvider,
    ScryfallProvider,
    WhatsInStandardProvider,
    WizardsProvider,
)
from .utils import parallel_call, url_keygen

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

    prints_api_json: Dict[str, Any] = ScryfallProvider().download(sf_prints_url)
    if prints_api_json["object"] == "error":
        LOGGER.error(f"No data found for {sf_prints_url}: {prints_api_json}")
        return []

    for foreign_card in prints_api_json["data"]:
        if (
            set_name != foreign_card["set"]
            or card_number != foreign_card["collector_number"]
            or foreign_card["lang"] == "en"
        ):
            continue

        card_foreign_entry = MtgjsonForeignDataObject()
        try:
            card_foreign_entry.language = LANGUAGE_MAP[foreign_card["lang"]]
        except IndexError:
            LOGGER.warning(f"Unable to get language {foreign_card}")

        if foreign_card["multiverse_ids"]:
            card_foreign_entry.multiverse_id = foreign_card["multiverse_ids"][0]

        if "card_faces" in foreign_card:
            if card_name.lower() == foreign_card["name"].split("/")[0].strip().lower():
                face = 0
            else:
                face = 1

            foreign_card = foreign_card["card_faces"][face]
            LOGGER.debug(f"Split card found: Using face {face} for {card_name}")

        card_foreign_entry.name = foreign_card.get("printed_name")
        card_foreign_entry.text = foreign_card.get("printed_text")
        card_foreign_entry.flavor_text = foreign_card.get("flavor_text")
        card_foreign_entry.type = foreign_card.get("printed_type_line")

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
        if value in SUPER_TYPES:
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
    total: float = 0

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


def uniquify_cards_with_same_name(mtgjson_cards: List[MtgjsonCardObject]) -> None:
    """
    Some sets (namely Un-sets) have cards with the same name, but different effects.
    This addresses it by adding (b), (c), etc to the name of duplicates
    :param mtgjson_cards: Cards object
    """

    if not mtgjson_cards:
        return

    if mtgjson_cards[0].set_code.upper() in {"HHO", "UNH"}:
        return

    if mtgjson_cards[0].border_color == "silver":
        cards_found_already: Dict[str, int] = {}

        for card in mtgjson_cards:
            cards_with_same_name_sum = sum(
                1 for item in mtgjson_cards if item.name == card.name
            )

            if card.name not in BASIC_LAND_NAMES and (
                card.name in cards_found_already.keys() or cards_with_same_name_sum > 1
            ):
                if card.name in cards_found_already.keys():
                    cards_found_already[card.name] += 1
                else:
                    cards_found_already[card.name] = ord("a")

                if cards_found_already[card.name] != ord("a"):
                    card.name += f" ({chr(cards_found_already[card.name])})"
                card.set_names(None)


def relocate_miscellaneous_tokens(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Sometimes tokens find their way into the main set. This will
    remove them from the cards array and sets an internal market
    to be dealt with later down the line
    :param mtgjson_set: MTGJSON Set object
    """
    token_types = {"token", "double_faced_token", "emblem", "art_series"}

    # Identify unique tokens from cards
    tokens_found = {
        card.scryfall_id for card in mtgjson_set.cards if card.layout in token_types
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


def mark_duel_decks(set_code: str, mtgjson_cards: List[MtgjsonCardObject]) -> None:
    """
    For Duel Decks, we need to determine which "deck" the card
    can be found in. This is a convoluted, but correct, approach
    at solving that problem.
    :param set_code: Set to work on
    :param mtgjson_cards: Card Objects
    """
    if set_code.startswith("DD") or set_code in {"GS1"}:
        land_pile_marked = False
        side_letter_as_number = ord("a")

        for card in sorted(mtgjson_cards):
            if card.name in BASIC_LAND_NAMES:
                land_pile_marked = True
            elif any(_type in card.type for _type in ("Token", "Emblem")):
                continue
            elif land_pile_marked:
                side_letter_as_number += 1
                land_pile_marked = False

            card.duel_deck = chr(side_letter_as_number)


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
    mtgjson_set.keyrune_code = pathlib.Path(set_data["icon_svg_uri"]).stem.upper()
    mtgjson_set.release_date = set_data["released_at"]
    mtgjson_set.mtgo_code = set_data.get("mtgo_code", "").upper()
    mtgjson_set.parent_code = set_data.get("parent_set_code", "")
    mtgjson_set.block = set_data.get("block", "")
    mtgjson_set.is_online_only = set_data.get("digital", "")
    mtgjson_set.is_foil_only = set_data.get("foil_only", "")
    mtgjson_set.search_uri = set_data["search_uri"]
    mtgjson_set.mcm_name = McmProvider().get_set_name(mtgjson_set.name)
    mtgjson_set.mcm_id = McmProvider().get_set_id(mtgjson_set.name)
    mtgjson_set.translations = WizardsProvider().get_translation_for_set(
        mtgjson_set.code
    )

    base_total_sizes = get_base_and_total_set_sizes(set_code)
    mtgjson_set.base_set_size = base_total_sizes[0]
    mtgjson_set.total_set_size = base_total_sizes[1]

    # Building cards is a process
    mtgjson_set.cards = build_base_mtgjson_cards(set_code)
    add_is_starter_option(set_code, mtgjson_set.search_uri, mtgjson_set.cards)
    uniquify_cards_with_same_name(mtgjson_set.cards)
    relocate_miscellaneous_tokens(mtgjson_set)
    add_variations_and_alternative_fields(mtgjson_set)
    add_mcm_details(mtgjson_set)

    # Build tokens, a little less of a process
    mtgjson_set.tokens = build_base_mtgjson_tokens(
        f"T{set_code}", mtgjson_set.extra_tokens or []
    )

    mtgjson_set.tcgplayer_group_id = set_data.get("tcgplayer_id")
    mtgjson_set.booster_v3 = get_booster_contents_v3(set_code)

    mark_duel_decks(set_code, mtgjson_set.cards)

    # Implicit Variables
    mtgjson_set.is_foreign_only = mtgjson_set.code in FOREIGN_SETS
    mtgjson_set.is_partial_preview = MtgjsonMetaObject().date < mtgjson_set.release_date

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


def build_base_mtgjson_cards(
    set_code: str, additional_cards: List[Dict[str, Any]] = None, is_token: bool = False
) -> List[MtgjsonCardObject]:
    """
    Construct all cards in MTGJSON format from a single set
    :param set_code: Set to build
    :param additional_cards: Additional objects to build (not relevant for normal builds)
    :param is_token: Are tokens being copmiled?
    :return: Completed card objects
    """
    cards = ScryfallProvider().download_cards(set_code)
    cards.extend(additional_cards or [])

    mtgjson_cards = parallel_call(
        build_mtgjson_card, cards, fold_list=True, repeatable_args=(0, is_token)
    )

    return list(mtgjson_cards)


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
    starter_card_url = search_url.replace("&unique=", "++not:booster&unique=")
    starter_cards = ScryfallProvider().download(starter_card_url)

    if starter_cards["object"] == "error":
        LOGGER.debug(f"All cards in {set_code} are available in boosters")
        return

    for scryfall_object in starter_cards["data"]:
        try:
            card = next(
                item
                for item in mtgjson_cards
                if item.scryfall_id == scryfall_object["id"]
            )
            if card:
                card.is_starter = True
        except StopIteration:
            pass


def add_leadership_skills(mtgjson_card: MtgjsonCardObject) -> None:
    """
    Determine if a card is able to be your commander, and if so
    which format(s).
    :param mtgjson_card: Card object
    """
    is_commander_legal = (
        "Legendary" in mtgjson_card.type
        and "Creature" in mtgjson_card.type
        # Exclude Flip cards
        and mtgjson_card.type not in {"flip"}
        # Exclude Melded cards and backside of Transform cards
        and (mtgjson_card.side == "a" if mtgjson_card.side else True)
    ) or ("can be your commander" in mtgjson_card.text)

    is_oathbreaker_legal = "Planeswalker" in mtgjson_card.type

    is_brawl_legal = mtgjson_card.set_code in WhatsInStandardProvider().set_codes and (
        is_oathbreaker_legal or is_commander_legal
    )

    if is_commander_legal or is_oathbreaker_legal or is_brawl_legal:
        mtgjson_card.leadership_skills = MtgjsonLeadershipSkillsObject(
            is_brawl_legal, is_commander_legal, is_oathbreaker_legal
        )


def add_uuid(mtgjson_card: MtgjsonCardObject, is_card: bool = True) -> None:
    """
    Construct a UUIDv5 for each MTGJSON card object
    :param is_card: Is this a card or token object
    :param mtgjson_card: Card object
    """
    if is_card:
        id_source = (
            ScryfallProvider().get_class_id()
            + mtgjson_card.scryfall_id
            + mtgjson_card.name
            + (mtgjson_card.face_name or "")
        )
    else:
        id_source = (
            mtgjson_card.name
            + (mtgjson_card.face_name or "")
            + "".join((mtgjson_card.colors or ""))
            + (mtgjson_card.power or "")
            + (mtgjson_card.toughness or "")
            + (mtgjson_card.side or "")
            + mtgjson_card.set_code[1:]
            + mtgjson_card.scryfall_id
        )

    mtgjson_card.uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source))


def build_mtgjson_card(
    scryfall_object: Dict[str, Any], face_id: int = 0, is_token: bool = False
) -> List[MtgjsonCardObject]:
    """
    Construct a MTGJSON Card object from 3rd party
    entities
    :param scryfall_object: Scryfall Card Object
    :param face_id: What face to build for (set internally)
    :param is_token: Is this a token object? (some diff fields)
    :return: List of card objects that were constructed
    """
    LOGGER.info(f"Building {scryfall_object['set'].upper()}: {scryfall_object['name']}")

    # Return List
    mtgjson_cards = []

    # Object Container
    mtgjson_card = MtgjsonCardObject(is_token)

    mtgjson_card.name = scryfall_object["name"]
    mtgjson_card.set_code = scryfall_object["set"]
    mtgjson_card.scryfall_id = scryfall_object["id"]
    mtgjson_card.scryfall_oracle_id = scryfall_object["oracle_id"]
    mtgjson_card.scryfall_illustration_id = scryfall_object.get("illustration_id")

    # Handle atypical cards
    face_data = scryfall_object
    if "card_faces" in scryfall_object:
        mtgjson_card.set_names(scryfall_object["name"].split("//"))

        # Override face_data from above
        face_data = scryfall_object["card_faces"][face_id]

        if "//" in scryfall_object.get("mana_cost", ""):
            mtgjson_card.colors = get_card_colors(
                scryfall_object["mana_cost"].split("//")[face_id]
            )
            mtgjson_card.face_converted_mana_cost = get_card_cmc(
                scryfall_object["mana_cost"].split("//")[face_id]
            )
        elif scryfall_object["layout"] in {
            "split",
            "transform",
            "aftermath",
            "adventure",
        }:
            mtgjson_card.face_converted_mana_cost = get_card_cmc(
                face_data.get("mana_cost", "0")
            )

        mtgjson_card.watermark = scryfall_object["card_faces"][0].get("watermark")

        if scryfall_object["card_faces"][-1]["oracle_text"].startswith("Aftermath"):
            mtgjson_card.layout = "aftermath"

        mtgjson_card.artist = scryfall_object["card_faces"][face_id].get("artist", "")

        if face_id == 0:
            for i in range(1, len(scryfall_object["card_faces"])):
                mtgjson_cards.extend(build_mtgjson_card(scryfall_object, i, is_token))

    # Start of single card builder
    if face_data.get("mana_cost"):
        mtgjson_card.mana_cost = face_data["mana_cost"]

    if not mtgjson_card.colors:
        mtgjson_card.colors = (
            face_data["colors"]
            if "colors" in face_data.keys()
            else scryfall_object["colors"]
        )

    # Explicit Variables -- Based on the entire card object

    mtgjson_card.border_color = scryfall_object.get("border_color", "")
    mtgjson_card.color_identity = scryfall_object.get("color_identity", "")
    mtgjson_card.converted_mana_cost = scryfall_object.get("cmc", "")
    mtgjson_card.edhrec_rank = scryfall_object.get("edhrec_rank")
    mtgjson_card.frame_effects = scryfall_object.get("frame_effects", "")
    mtgjson_card.frame_version = scryfall_object.get("frame", "")
    mtgjson_card.hand = scryfall_object.get("hand_modifier")
    mtgjson_card.has_foil = scryfall_object.get("foil")
    mtgjson_card.has_non_foil = scryfall_object.get("nonfoil")
    mtgjson_card.is_buy_a_box = "buyabox" in scryfall_object.get("promo_types", [])
    mtgjson_card.is_date_stamped = "datestamped" in scryfall_object.get(
        "promo_types", []
    )
    mtgjson_card.is_full_art = scryfall_object.get("full_art")
    mtgjson_card.is_online_only = scryfall_object.get("digital")
    mtgjson_card.is_oversized = scryfall_object.get("oversized")
    mtgjson_card.is_promo = scryfall_object.get("promo")
    mtgjson_card.is_reprint = scryfall_object.get("reprint")
    mtgjson_card.is_reserved = scryfall_object.get("reserved")
    mtgjson_card.is_story_spotlight = scryfall_object.get("story_spotlight")
    mtgjson_card.is_textless = scryfall_object.get("textless")
    mtgjson_card.life = scryfall_object.get("life_modifier")
    mtgjson_card.mtg_arena_id = scryfall_object.get("arena_id")
    mtgjson_card.mtgo_id = scryfall_object.get("mtgo_id")
    mtgjson_card.mtgo_foil_id = scryfall_object.get("mtgo_foil_id")
    mtgjson_card.number = scryfall_object.get("collector_number", "0")

    mtgjson_card.raw_purchase_urls = scryfall_object.get("purchase_uris", {})
    mtgjson_card.tcgplayer_product_id = scryfall_object.get("tcgplayer_id", 0)
    mtgjson_card.purchase_urls.tcgplayer = url_keygen(mtgjson_card.tcgplayer_product_id)

    mtgjson_card.rarity = scryfall_object.get("rarity", "")
    if not mtgjson_card.artist:
        mtgjson_card.artist = scryfall_object.get("artist", "")
    if not mtgjson_card.layout:
        mtgjson_card.layout = scryfall_object.get("layout", "")
    if not mtgjson_card.watermark:
        mtgjson_card.watermark = face_data.get("watermark")

    for game_mode in scryfall_object.get("games", []):
        # isPaper, isMtgo, isArena
        setattr(mtgjson_card, f"is{game_mode.capitalize()}", True)

    # Explicit Variables -- Based on the face of the card
    mtgjson_card.loyalty = face_data.get("loyalty")

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
            mtgjson_card.multiverse_id = scryfall_object["multiverse_ids"][face_id]
        else:
            mtgjson_card.multiverse_id = scryfall_object["multiverse_ids"][0]

    # Add "side" for split cards (cards with exactly 2 sides)
    # Also set face name
    if mtgjson_card.get_names():
        mtgjson_card.face_name = str(face_data["name"])

        if mtgjson_card.layout not in ["meld"]:
            # chr(97) = 'a', chr(98) = 'b', ...
            mtgjson_card.side = chr(
                mtgjson_card.get_names().index(mtgjson_card.face_name) + 97
            )

    # Implicit Variables
    mtgjson_card.is_timeshifted = (
        scryfall_object.get("frame") == "future" or mtgjson_card.set_code == "TSB"
    )
    mtgjson_card.printings = parse_printings(
        scryfall_object["prints_search_uri"].replace("%22", "")
    )
    mtgjson_card.legalities = parse_legalities(scryfall_object["legalities"])
    mtgjson_card.rulings = parse_rulings(scryfall_object["rulings_uri"])

    card_types = parse_card_types(mtgjson_card.type)
    mtgjson_card.supertypes = card_types[0]
    mtgjson_card.types = card_types[1]
    mtgjson_card.subtypes = card_types[2]

    if "Planeswalker" in mtgjson_card.types:
        mtgjson_card.text = re.sub(r"([+−-]?[0-9]+):", r"[\1]:", mtgjson_card.text)

    # Handle Meld components, as well as tokens
    if "all_parts" in scryfall_object.keys():
        meld_object = []
        mtgjson_card.set_names(None)
        for a_part in scryfall_object["all_parts"]:
            if a_part["component"] != "token":
                if "//" in a_part.get("name"):
                    mtgjson_card.set_names(a_part.get("name").split("//"))
                    break

                # This is a meld only-fix, so we ignore tokens/combo pieces
                if "meld" in a_part["component"]:
                    meld_object.append(a_part["component"])
                    mtgjson_card.append_names(a_part.get("name"))

        # If the only entry is the original card, empty the names array
        if (
            mtgjson_card.get_names()
            and len(mtgjson_card.get_names()) == 1
            and mtgjson_card.name in mtgjson_card.get_names()
        ):
            mtgjson_card.set_names(None)

        # Meld cards should be CardA, Meld, CardB.
        if (
            len(meld_object) == 3
            and meld_object[1] != "meld_result"
            and mtgjson_card.get_names()
        ):
            mtgjson_card.set_names(
                [
                    mtgjson_card.get_names()[0],
                    mtgjson_card.get_names()[2],
                    mtgjson_card.get_names()[1],
                ]
            )

        # Meld Object
        if mtgjson_card.get_names() and len(mtgjson_card.get_names()) == 3:
            if mtgjson_card.name == mtgjson_card.get_names()[0]:
                mtgjson_card.side = "a"
            elif mtgjson_card.name == mtgjson_card.get_names()[2]:
                mtgjson_card.side = "b"
            else:
                mtgjson_card.side = "c"

    mtgjson_card.foreign_data = parse_foreign(
        scryfall_object["prints_search_uri"].replace("%22", ""),
        mtgjson_card.name,
        mtgjson_card.number,
        mtgjson_card.set_code,
    )

    if mtgjson_card.name in ScryfallProvider().cards_without_limits:
        mtgjson_card.has_alternative_deck_limit = True

    add_uuid(mtgjson_card)
    add_leadership_skills(mtgjson_card)

    if is_token:
        reverse_related: List[str] = []
        if "all_parts" in scryfall_object:
            for a_part in scryfall_object["all_parts"]:
                if a_part.get("name") != mtgjson_card.name:
                    reverse_related.append(a_part.get("name"))
        mtgjson_card.reverse_related = reverse_related

    # Gatherer Calls -- SLOWWWWW
    if mtgjson_card.multiverse_id:
        gatherer_cards = GathererProvider().get_cards(
            mtgjson_card.multiverse_id, mtgjson_card.set_code
        )
        if len(gatherer_cards) > face_id:
            mtgjson_card.original_type = gatherer_cards[face_id].original_types
            mtgjson_card.original_text = gatherer_cards[face_id].original_text

    mtgjson_cards.append(mtgjson_card)

    return mtgjson_cards


def add_variations_and_alternative_fields(mtgjson_set: MtgjsonSetObject) -> None:
    """
    For non-silver bordered sets, we will create a "variations"
    field will be created that has UUID of repeat cards.
    This will also mark alternative printings within a single set.
    This will also set the otherFaceIds list.
    :param mtgjson_set: <<CONST>> object for the file
    :return: How many alternative printings were marked
    """
    if not mtgjson_set.cards:
        return

    if mtgjson_set.cards[0].border_color != "silver" or mtgjson_set.code in {
        "HHO",
        "UNH",
        "UST",
    }:
        for card in mtgjson_set.cards:
            # Adds other face ID list
            if card.get_names():
                card.other_face_ids = [
                    card_obj.uuid
                    for card_obj in mtgjson_set.cards
                    if card_obj.face_name in card.get_names()
                    and card_obj.uuid != card.uuid
                ]

            # Adds variations
            variations = [
                item.uuid
                for item in mtgjson_set.cards
                if item.name == card.name
                and item.face_name == card.face_name
                and item.uuid != card.uuid
            ]

            if variations:
                card.variations = variations

            # Add alternative tag
            # Ignore singleton printings in set, as well as basics
            if not variations or card.name in BASIC_LAND_NAMES:
                continue

            # Some hardcoded checking due to inconsistencies upstream
            if mtgjson_set.code.upper() in ["UNH", "10E"]:
                # Check for duplicates, mark the foils
                if len(variations) >= 1 and card.has_foil and not card.has_non_foil:
                    card.is_alternative = True
            elif mtgjson_set.code.upper() in ["CN2", "BBD"]:
                # Check for set number > set size
                if int(card.number.replace(chr(9733), "")) > mtgjson_set.base_set_size:
                    card.is_alternative = True
            else:
                # Check for a star in the number
                if chr(9733) in card.number:
                    card.is_alternative = True


def add_mcm_details(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Add the MKM components to a set's cards and tokens
    :param mtgjson_set: MTGJSON Set
    """
    mkm_cards = McmProvider().get_mkm_cards(mtgjson_set.mcm_id)
    for mtgjson_card in mtgjson_set.cards:
        for key, mkm_obj in mkm_cards.items():
            if mtgjson_card.name.lower() not in key:
                continue

            if "number" not in mkm_obj.keys() or (
                mkm_obj["number"] in mtgjson_card.number
            ):
                mtgjson_card.mcm_id = mkm_obj["idProduct"]
                mtgjson_card.mcm_meta_id = mkm_obj["idMetaproduct"]

                mtgjson_card.purchase_urls.cardmarket = url_keygen(
                    str(mtgjson_card.mcm_id)
                    + CARD_MARKET_BUFFER
                    + str(mtgjson_card.mcm_meta_id)
                )

                break


def get_base_and_total_set_sizes(set_code: str) -> Tuple[int, int]:
    """
    Get the size of a set from scryfall or corrections file
    :param set_code: Set code, upper case
    :return: Amount of cards in set (base, total)
    """
    # Load cache if not loaded
    with RESOURCE_PATH.joinpath("base_set_sizes.json").open(encoding="utf-8") as f:
        base_set_size_override = json.load(f)

    if set_code in base_set_size_override.keys():
        # Manual correction
        base_set_size = int(base_set_size_override[set_code])
    else:
        # Download on the fly
        base_set_size_download = ScryfallProvider().download(
            ScryfallProvider().CARDS_IN_BASE_SET_URL.format(set_code)
        )
        if base_set_size_download["object"] == "error":
            LOGGER.warning(f"Unable to get base(2) set size for {set_code}")
            base_set_size = 0
        else:
            base_set_size = int(base_set_size_download["total_cards"])

    total_set_size = len(ScryfallProvider().download_cards(set_code))

    return base_set_size, total_set_size


def get_booster_contents_v3(set_code: str) -> Optional[List[Any]]:
    """
    Grab the MTGJSON specific booster contents (supporting V3)
    :param set_code: What set to find
    :return Booster list or None
    """
    with RESOURCE_PATH.joinpath("boosters.json").open(encoding="utf-8") as f:
        json_dict: Dict[str, List[Any]] = json.load(f)
        if set_code in json_dict.keys():
            return json_dict[set_code]
    return None
