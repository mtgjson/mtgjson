"""
MTGJSON Set Builder
"""
import itertools
import logging
import multiprocessing
import pathlib
import re
import unicodedata
from typing import Dict, Any, Optional, List, Set, Tuple

from mtgjson5.classes.mtgjson_card_obj import MtgjsonCardObject
from mtgjson5.classes.mtgjson_meta_obj import MtgjsonMetaObject
from mtgjson5.classes.mtgjson_set_obj import MtgjsonSetObject
from mtgjson5.globals import init_logger, FOREIGN_SETS, SUPER_TYPES
from mtgjson5.providers.scryfall_provider import ScryfallProvider


def parse_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Given a card type string, split it up into its raw components: super, sub, and type
    :param card_type: Card type string to parse
    :return: Tuple (super, type, sub) of the card's attributes
    """
    sub_types: List[str] = []
    super_types: List[str] = []
    types: List[str] = []

    if "—" not in card_type:
        supertypes_and_types = card_type
    else:
        split_type: List[str] = card_type.split("—")
        supertypes_and_types: str = split_type[0]
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
    scryfall_instance = ScryfallProvider.instance()
    set_data: Dict[str, Any] = scryfall_instance.download(
        scryfall_instance.ALL_SETS + set_code
    )

    if set_data["object"] == "error":
        logging.error(f"Failed to download {set_code}")
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

    symbol: List[str] = re.findall(r"{([^{]*)}", mana_cost)
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


def parse_printings(sf_prints_url: str) -> List[str]:
    """
    Given a Scryfall printings URL, extract all sets a card was printed in
    :param sf_prints_url: URL to extract data from
    :return: List of all sets a specific card was printed in
    """
    card_sets: Set[str] = set()

    while sf_prints_url:
        prints_api_json: Dict[str, Any] = ScryfallProvider.instance().download(
            sf_prints_url
        )

        if prints_api_json["object"] == "error":
            logging.error(f"Bad download: {sf_prints_url}")
            break

        for card in prints_api_json["data"]:
            card_sets.add(card.get("set").upper())

        if not prints_api_json.get("has_more"):
            break

        sf_prints_url = prints_api_json.get("next_page")

    return list(card_sets)


def parse_legalities(sf_card_legalities: Dict[str, str]) -> Dict[str, str]:
    """
    Given a Scryfall legalities dictionary, convert it to MTGJSON format
    :param sf_card_legalities: Scryfall legalities
    :return: MTGJSON legalities
    """
    card_legalities: Dict[str, str] = {}
    for key, value in sf_card_legalities.items():
        if value != "not_legal":
            card_legalities[key] = value.capitalize()

    return card_legalities


def parse_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = ScryfallProvider.instance().download(rulings_url)
    if rules_api_json["object"] == "error":
        logging.error(f"Error downloading URL {rulings_url}: {rules_api_json}")
        return []

    mtgjson_rules: List[Dict[str, str]] = []

    sf_rules = [rule for rule in rules_api_json["data"]]
    for sf_rule in sf_rules:
        mtgjson_rule: Dict[str, str] = {
            "date": sf_rule["published_at"],
            "text": sf_rule["comment"],
        }
        mtgjson_rules.append(mtgjson_rule)

    return mtgjson_rules


def build_mtgjson_set(set_code: str, is_privileged: bool = True) -> MtgjsonSetObject:
    """
    :param is_privileged:
    :param set_code:
    :return:
    """
    init_logger()

    # Output Object
    mtgjson_set = MtgjsonSetObject()

    # Ensure we have a header for this set
    set_data = get_scryfall_set_data(set_code)
    if not set_data:
        return mtgjson_set

    # Explicit Variables
    mtgjson_set.name = set_data["name"]
    mtgjson_set.code = set_data["code"].upper()
    mtgjson_set.type = set_data["set_type"]
    mtgjson_set.keyrune_code = pathlib.Path(set_data["icon_svg_uri"]).stem.upper()
    mtgjson_set.release_date = set_data["released_at"]
    mtgjson_set.mtgo_code = set_data.get("mtgo_code")
    mtgjson_set.parent_code = set_data.get("parent_set_code")
    mtgjson_set.block = set_data.get("block")
    mtgjson_set.is_online_only = set_data.get("digital")
    mtgjson_set.is_foil_only = set_data.get("foil_only")
    mtgjson_set.meta = MtgjsonMetaObject()
    mtgjson_set.search_uri = set_data.get("search_uri")

    mtgjson_set.cards = build_mtgjson_cards(set_code, is_privileged)

    # Implicit Variables
    mtgjson_set.is_foreign_only = mtgjson_set.code in FOREIGN_SETS
    mtgjson_set.is_partial_preview = (
        mtgjson_set.meta.date.strftime("%Y-%m-%d") < mtgjson_set.release_date
    )

    return mtgjson_set


def build_mtgjson_cards(
    set_code: str, is_privileged: bool = True
) -> List[MtgjsonCardObject]:
    """
    Construct all cards in MTGJSON format from a single set
    :param set_code: Set to build
    :param is_privileged: Should we download with Authorization
    :return: Completed card objects
    """
    cards = ScryfallProvider.instance().download_cards(set_code, in_booster=True)

    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        temp_cards = pool.map(build_mtgjson_card, cards)

    mtgjson_cards = list(itertools.chain.from_iterable(temp_cards))
    return mtgjson_cards


def build_mtgjson_card(
    scryfall_object: Dict[str, Any], face_id: int = 0
) -> List[MtgjsonCardObject]:
    """
    Construct a MTGJSON Card object from 3rd party
    entities
    :param scryfall_object: Scryfall Set Object
    :param is_privileged: Should we download with Authorization
    :param face_id: What face to build for (set internally)
    :return: List of card objects that were constructed
    """
    logging.info(f"Building {scryfall_object['name']}")

    mtgjson_cards = []

    mtgjson_card = MtgjsonCardObject()
    mtgjson_card._set_code = scryfall_object["set"]
    mtgjson_card.scryfall_id = scryfall_object["id"]
    mtgjson_card.scryfall_oracle_id = scryfall_object["oracle_id"]
    mtgjson_card.scryfall_illustration_id = scryfall_object.get("illustration_id")

    # Handle atypical cards
    face_data = scryfall_object
    if "card_faces" in scryfall_object:
        mtgjson_card.names = scryfall_object["name"].split(" // ")

        # Update face_data for later uses
        face_data = scryfall_object["card_faces"][face_id]

        if "//" in scryfall_object.get("mana_cost", ""):
            mtgjson_card.colors = get_card_colors(
                scryfall_object["mana_cost"].split(" // ")[face_id]
            )
            mtgjson_card.face_converted_mana_cost = get_card_cmc(
                scryfall_object["mana_cost"].split("//")[face_id].strip()
            )
        elif scryfall_object["layout"] in [
            "split",
            "transform",
            "aftermath",
            "adventure",
        ]:
            mtgjson_card.face_converted_mana_cost = get_card_cmc(
                face_data.get("mana_cost", "0").strip()
            )

        mtgjson_card.watermark = scryfall_object["card_faces"][0].get("watermark")
        if scryfall_object["card_faces"][-1]["oracle_text"].startswith("Aftermath"):
            mtgjson_card.layout = "aftermath"

        mtgjson_card.artist = scryfall_object["card_faces"][face_id].get("artist")

        if face_id == 0:
            for i in range(1, len(scryfall_object["card_faces"])):
                mtgjson_cards.extend(build_mtgjson_card(scryfall_object, i))

    if face_data.get("mana_cost"):
        mtgjson_card.mana_cost = face_data["mana_cost"]

    if not mtgjson_card.colors:
        mtgjson_card.colors = (
            face_data["colors"]
            if "colors" in face_data.keys()
            else scryfall_object["colors"]
        )

    # Explicit Variables -- Based on the entire card object
    mtgjson_card.border_color = scryfall_object.get("border_color")
    mtgjson_card.color_identity = scryfall_object.get("color_identity")
    mtgjson_card.converted_mana_cost = scryfall_object.get("cmc")
    mtgjson_card.edhrec_rank = scryfall_object.get("edhrec_rank")
    mtgjson_card.frame_effect = scryfall_object.get("frame_effects", [""])[0]
    mtgjson_card.frame_effects = scryfall_object.get("frame_effects")
    mtgjson_card.frame_version = scryfall_object.get("frame")
    mtgjson_card.hand = scryfall_object.get("hand_modifier")
    mtgjson_card.has_foil = scryfall_object.get("foil")
    mtgjson_card.has_non_foil = scryfall_object.get("nonfoil")
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
    mtgjson_card.number = scryfall_object.get("collector_number")
    mtgjson_card.tcgplayer_product_id = scryfall_object.get("tcgplayer_id")
    mtgjson_card.rarity = scryfall_object.get("rarity")
    if not mtgjson_card.artist:
        mtgjson_card.artist = scryfall_object.get("artist")
    if not mtgjson_card.layout:
        mtgjson_card.layout = scryfall_object.get("layout")

    # isPaper, isMtgo, isArena
    for game_mode in scryfall_object.get("games", []):
        setattr(mtgjson_card, f"is{game_mode.capitalize()}", True)

    # Explicit Variables -- Based on the face of the card
    mtgjson_card.loyalty = face_data.get("loyalty")
    mtgjson_card.name = face_data.get("name")
    mtgjson_card.power = face_data.get("power")
    mtgjson_card.text = face_data.get("oracle_text")
    mtgjson_card.toughness = face_data.get("toughness")
    mtgjson_card.type = face_data.get("type_line", "Card")
    if not mtgjson_card.watermark:
        mtgjson_card.watermark = face_data.get("watermark")

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
    if mtgjson_card.names and len(mtgjson_card.names) == 2:
        # chr(97) = 'a', chr(98) = 'b', ...
        mtgjson_card.side = chr(mtgjson_card.names.index(mtgjson_card.name) + 97)

    # Implicit Variables
    mtgjson_card.is_timeshifted = (
        scryfall_object.get("frame") == "future" or mtgjson_card._set_code == "TSB"
    )
    mtgjson_card.printings = parse_printings(scryfall_object["prints_search_uri"])
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
        mtgjson_card.names = []
        for a_part in scryfall_object["all_parts"]:
            if a_part["component"] != "token":
                if "//" in a_part.get("name"):
                    mtgjson_card.names = a_part.get("name").split(" // ")
                    break

                # This is a meld only-fix, so we ignore tokens/combo pieces
                if "meld" in a_part["component"]:
                    meld_object.append(a_part["component"])
                    mtgjson_card.names.append(a_part.get("name"))

        # If the only entry is the original card, empty the names array
        if len(mtgjson_card.names) == 1 and mtgjson_card.name in mtgjson_card.names:
            mtgjson_card.names = None

        # Meld cards should be CardA, Meld, CardB.
        if meld_object and meld_object[1] != "meld_result":
            mtgjson_card.names[1], mtgjson_card.names[2] = (
                mtgjson_card.names[2],
                mtgjson_card.names[1],
            )

        # Meld Object
        if len(mtgjson_card.names) == 3:
            if mtgjson_card.name == mtgjson_card.names[0]:
                mtgjson_card.side = "a"
            elif mtgjson_card.name == mtgjson_card.names[2]:
                mtgjson_card.side = "b"
            else:
                mtgjson_card.side = "c"

    mtgjson_cards.append(mtgjson_card)

    return mtgjson_cards
