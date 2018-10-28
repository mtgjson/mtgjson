"""Compile incoming data into the target output format."""

import logging
import multiprocessing
import re
from typing import Any, Dict, List, Tuple

import mtgjson4
from mtgjson4.provider import gatherer, scryfall

LOGGER = logging.getLogger(__name__)


def build_output_file(sf_cards: List[Dict[str, Any]], set_code: str) -> Dict[str, Any]:
    """
    Compile the entire XYZ.json file and pass it off to be written out
    :param sf_cards: Scryfall cards
    :param set_code: Set code
    :return: Completed JSON file
    """
    output_file: Dict[str, Any] = {}

    # Get the set config from ScryFall
    set_config = scryfall.download(scryfall.SCRYFALL_API_SETS + set_code)
    if set_config["object"] == "error":
        LOGGER.error("Set Config for {} was not found, skipping...".format(set_code))
        return {"cards": []}

    output_file["name"] = set_config.get("name")
    output_file["code"] = set_config.get("code")
    output_file["mtgoCode"] = set_config.get("mtgo_code")
    output_file["releaseDate"] = set_config.get("released_at")
    output_file["type"] = set_config.get("set_type")
    # output_file['booster'] = ''  # Maybe will re-add

    if set_config.get("block"):
        output_file["block"] = set_config.get("block")

    if set_config.get("digital"):
        output_file["isOnlineOnly"] = True

    if set_config.get("foil_only"):
        output_file["isFoilOnly"] = True

    # Declare the version of the build in the output file
    output_file["meta"] = {
        "version": mtgjson4.__VERSION__,
        "date": mtgjson4.__VERSION_DATE__,
    }

    LOGGER.info("Starting cards for {}".format(set_code))

    card_holder = convert_to_mtgjson(sf_cards)
    card_holder = add_starter_flag(set_code, set_config["search_uri"], card_holder)
    output_file["cards"] = card_holder

    LOGGER.info("Finished cards for {}".format(set_code))

    LOGGER.info("Starting tokens for {}".format(set_code))
    sf_tokens: List[Dict[str, Any]] = scryfall.get_set("t" + set_code)
    output_file["tokens"] = build_mtgjson_tokens(sf_tokens)
    LOGGER.info("Finished tokens for {}".format(set_code))

    return output_file


def add_starter_flag(
    set_code: str, search_url: str, mtgjson_cards: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Since SF doesn't provide individual card notices, we can post-process add the starter flag
    :param set_code: Set to address
    :param search_url: URL to fix up to get non-booster cards
    :param mtgjson_cards: Modify the argument and return it
    :return: List of cards
    """
    starter_card_url = search_url.replace("&unique=", "++not:booster&unique=")
    starter_cards = scryfall.download(starter_card_url)

    if starter_cards["object"] == "error":
        LOGGER.info("All cards in {} are available in boosters".format(set_code))
        return mtgjson_cards

    for sf_card in starter_cards["data"]:
        # Each card has a unique UUID, even if they're the same card printed twice
        try:
            card = next(item for item in mtgjson_cards if item["uuid"] == sf_card["id"])
            if card:
                card["starter"] = True
        except StopIteration:
            LOGGER.warning(
                "Passed on {0} with UUID {1}".format(sf_card["name"], sf_card["id"])
            )

    return mtgjson_cards


def build_mtgjson_tokens(sf_tokens: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Scryfall tokens to MTGJSON tokens
    :param sf_tokens: All tokens in a set
    :return: List of MTGJSON tokens
    """
    token_cards: List[Dict[str, Any]] = []

    for sf_token in sf_tokens:
        token_card: Dict[str, Any] = {
            "name": sf_token.get("name"),
            "type": sf_token.get("type_line"),
            "text": sf_token.get("oracle_text"),
            "power": sf_token.get("power"),
            "colors": sf_token.get("colors"),
            "colorIdentity": sf_token.get("color_identity"),
            "toughness": sf_token.get("toughness"),
            "loyalty": sf_token.get("loyalty"),
            "watermark": sf_token.get("watermark"),
            "uuid": sf_token.get("id"),
            "borderColor": sf_token.get("border_color"),
            "artist": sf_token.get("artist"),
            "isOnlineOnly": sf_token.get("digital"),
            "number": sf_token.get("collector_number"),
        }

        reverse_related: List[str] = []
        if "all_parts" in sf_token:
            for a_part in sf_token["all_parts"]:
                if a_part.get("name") != token_card.get("name"):
                    reverse_related.append(a_part.get("name"))
        token_card["reverseRelated"] = reverse_related

        LOGGER.info(
            "Parsed {0} from {1}".format(token_card.get("name"), sf_token.get("set"))
        )
        token_cards.append(token_card)

    return token_cards


def convert_to_mtgjson(sf_cards: List[Dict[str, Any]]) -> List[Any]:
    """
    Parallel method to build each card in the set
    :param sf_cards: cards to build
    :return: list of cards built
    """
    with multiprocessing.Pool(processes=8) as pool:
        results: List[Any] = pool.map(build_mtgjson_card, sf_cards)

        all_cards: List[Dict[str, Any]] = []
        for cards in results:
            for card in cards:
                all_cards.append(card)
    return all_cards


def remove_unnecessary_fields(card_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove invalid field entries to shrink JSON output size
    """

    fixed_dict: List[Dict[str, Any]] = []
    remove_field_if_false: List[str] = [
        "reserved",
        "isOversized",
        "isOnlineOnly",
        "timeshifted",
    ]

    for card_entry in card_list:
        insert_value = {}

        for key, value in card_entry.items():
            if value is not None:
                if key in remove_field_if_false and value is False:
                    continue
                if key == "foreignData":
                    value = fix_foreign_entries(value)

                insert_value[key] = value

        fixed_dict.append(insert_value)

    return fixed_dict


def fix_foreign_entries(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Foreign entries may have bad values, such as missing flavor text. This removes them.
    :param values: List of foreign entries dicts
    :return: Pruned foreign entries
    """
    # List of dicts
    fd_insert_list = []
    for foreign_info in values:
        fd_insert_dict = {}
        for fd_key, fd_value in foreign_info.items():
            if fd_value is not None:
                fd_insert_dict[fd_key] = fd_value

        fd_insert_list.append(fd_insert_dict)
    return fd_insert_list


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


def get_cmc(mana_cost: str) -> float:
    """
    For some cards, we may have to manually update the converted mana cost.
    We do this by reading the inner components of each pair of {} and
    deciphering what the contents mean. If number, we're good. Otherwise +1.
    :param mana_cost: Mana cost string
    :return: One sided cmc
    """
    total: float = 0

    symbol: List[str] = re.findall(r"{([\s\S]*?)}", mana_cost)
    for element in symbol:
        element = element.split("/")[0]  # Address 2/W, G/W, etc as "higher" cost always first
        if isinstance(element, (int, float)):
            total += float(element)
        elif element in ["X", "Y", "Z"]:  # Placeholder mana
            continue
        elif element[0] == "H":  # Half mana
            total += 0.5
        else:
            total += 1

    return total


def build_mtgjson_card(  # pylint: disable=too-many-branches
    sf_card: Dict[str, Any], sf_card_face: int = 0
) -> List[Dict[str, Any]]:
    """
    Build a mtgjson card (and all sub pieces of that card)
    :param sf_card: Card to build
    :param sf_card_face: Which part of the card (defaults to 0)
    :return: List of card(s) build (usually 1)
    """
    mtgjson_cards: List[Dict[str, Any]] = []
    mtgjson_card: Dict[str, Any] = {}

    # Let us know what card we're trying to parse -- good for debugging :)
    LOGGER.info("Parsing {0} from {1}".format(sf_card.get("name"), sf_card.get("set")))

    # If flip-type, go to card_faces for alt attributes
    face_data: Dict[str, Any] = sf_card

    if "card_faces" in sf_card:
        mtgjson_card["names"] = sf_card["name"].split(" // ")  # List[str]
        face_data = sf_card["card_faces"][sf_card_face]

        # Prevent duplicate UUIDs for split card halves
        # Remove the last character and replace with the id of the card face
        mtgjson_card["uuid"] = sf_card["id"][:-1] + str(sf_card_face)

        # Split cards and rotational cards have this field, flip cards do not.
        # Remove rotational cards via the additional check
        if "mana_cost" in sf_card and "//" in sf_card["mana_cost"]:
            mtgjson_card["colors"] = get_card_colors(
                sf_card["mana_cost"].split(" // ")[sf_card_face]
            )
            mtgjson_card["convertedManaCost"] = get_cmc(
                sf_card["mana_cost"].split(" // ")[sf_card_face]
            )

        # Recursively parse the other cards within this card too
        # Only call recursive if it is the first time we see this card object
        if sf_card_face == 0:
            for i in range(1, len(sf_card["card_faces"])):
                LOGGER.info(
                    "Parsing additional card {0} face {1}".format(
                        sf_card.get("name"), i
                    )
                )
                mtgjson_cards += build_mtgjson_card(sf_card, i)

    # Characteristics that can are not shared to both sides of flip-type cards
    if face_data.get("mana_cost"):
        mtgjson_card["manaCost"] = face_data.get("mana_cost")  # str

    if "colors" not in mtgjson_card:
        mtgjson_card["colors"] = face_data.get("colors")  # List[str]

    mtgjson_card["name"] = face_data.get("name")  # str
    mtgjson_card["type"] = face_data.get("type_line")  # str
    mtgjson_card["text"] = face_data.get("oracle_text")  # str

    mtgjson_card["power"] = face_data.get("power")  # str
    mtgjson_card["toughness"] = face_data.get("toughness")  # str
    mtgjson_card["loyalty"] = face_data.get("loyalty")  # str
    mtgjson_card["watermark"] = face_data.get("watermark")  # str

    if "color_indicator" in face_data:
        mtgjson_card["colorIndicator"] = face_data.get("color_indicator")  # List[str]
    elif "color_indicator" in sf_card:
        mtgjson_card["colorIndicator"] = sf_card.get("color_indicator")  # List[str]

    try:
        mtgjson_card["multiverseId"] = sf_card["multiverse_ids"][sf_card_face]  # int
    except IndexError:
        try:
            mtgjson_card["multiverseId"] = sf_card["multiverse_ids"][0]  # int
        except IndexError:
            mtgjson_card["multiverseId"] = None  # int

    # Characteristics that are shared to all sides of flip-type cards, that we don't have to modify
    mtgjson_card["artist"] = sf_card.get("artist")  # str
    mtgjson_card["borderColor"] = sf_card.get("border_color")
    mtgjson_card["colorIdentity"] = sf_card.get("color_identity")  # List[str]

    if "convertedManaCost" not in mtgjson_card:
        mtgjson_card["convertedManaCost"] = sf_card.get("cmc")  # float

    mtgjson_card["flavorText"] = sf_card.get("flavor_text")  # str
    mtgjson_card["frameVersion"] = sf_card.get("frame")  # str
    mtgjson_card["hasFoil"] = sf_card.get("foil")  # bool
    mtgjson_card["hasNonFoil"] = sf_card.get("nonfoil")  # bool
    mtgjson_card["isOnlineOnly"] = sf_card.get("digital")  # bool
    mtgjson_card["isOversized"] = sf_card.get("oversized")  # bool
    mtgjson_card["layout"] = sf_card.get("layout")  # str
    mtgjson_card["number"] = sf_card.get("collector_number")  # str
    mtgjson_card["isReserved"] = sf_card.get("reserved")  # bool

    if "uuid" not in mtgjson_card:
        mtgjson_card["uuid"] = sf_card.get("id")  # str

    # Characteristics that we have to format ourselves from provided data
    mtgjson_card["timeshifted"] = sf_card.get("timeshifted") or sf_card.get(
        "futureshifted"
    )  # bool
    mtgjson_card["rarity"] = (
        sf_card.get("rarity") if not mtgjson_card.get("timeshifted") else "Special"
    )  # str

    # Characteristics that we need custom functions to parse
    print_search_url: str = sf_card["prints_search_uri"].replace("%22", "")
    mtgjson_card["legalities"] = scryfall.parse_legalities(
        sf_card["legalities"]
    )  # Dict[str, str]
    mtgjson_card["rulings"] = sorted(
        scryfall.parse_rulings(sf_card["rulings_uri"]),
        key=lambda ruling: ruling["date"],
    )
    mtgjson_card["printings"] = sorted(
        scryfall.parse_printings(print_search_url)
    )  # List[str]

    card_types: Tuple[List[str], List[str], List[str]] = scryfall.parse_card_types(
        mtgjson_card["type"]
    )
    mtgjson_card["supertypes"] = card_types[0]  # List[str]
    mtgjson_card["types"] = card_types[1]  # List[str]
    mtgjson_card["subtypes"] = card_types[2]  # List[str]

    # Handle meld issues
    if "all_parts" in sf_card:
        mtgjson_card["names"] = []
        for a_part in sf_card["all_parts"]:
            mtgjson_card["names"].append(a_part.get("name"))

    # Characteristics that we cannot get from Scryfall
    # Characteristics we have to do further API calls for
    mtgjson_card["foreignData"] = scryfall.parse_foreign(
        print_search_url, mtgjson_card["name"], sf_card["set"]
    )

    if mtgjson_card["multiverseId"] is not None:
        gatherer_cards = gatherer.get_cards(mtgjson_card["multiverseId"])
        try:
            gatherer_card = gatherer_cards[sf_card_face]
            mtgjson_card["originalType"] = gatherer_card.original_types
            mtgjson_card["originalText"] = gatherer_card.original_text
        except IndexError:
            LOGGER.warning("Unable to parse originals for {}".format(mtgjson_card["name"]))

    mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards
