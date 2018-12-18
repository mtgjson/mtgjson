"""Compile incoming data into the target output format."""

import contextvars
import copy
import json
import logging
import multiprocessing
import pathlib
import re
from typing import Any, Dict, List, Set, Tuple
import uuid

import mtgjson4
from mtgjson4.provider import gatherer, scryfall, tcgplayer
from mtgjson4.util import is_number

LOGGER = logging.getLogger(__name__)

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")


def build_output_file(
    sf_cards: List[Dict[str, Any]], set_code: str, skip_tcgplayer: bool
) -> Dict[str, Any]:
    """
    Compile the entire XYZ.json file and pass it off to be written out
    :param skip_tcgplayer: Skip building TCGPlayer stuff
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

    # Add booster info based on boosters resource (manually maintained for the time being)
    with pathlib.Path(mtgjson4.RESOURCE_PATH, "boosters.json").open(
        "r", encoding="utf-8"
    ) as f:
        json_dict: Dict[str, List[Any]] = json.load(f)
        if output_file["code"].upper() in json_dict.keys():
            output_file["boosterV3"] = json_dict[output_file["code"].upper()]

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
    card_holder, non_booster_cards = add_start_flag_and_count_modified(
        set_code, set_config["search_uri"], card_holder
    )

    # Address duplicates in un-sets
    card_holder = uniquify_duplicates_in_set(card_holder)

    # Move bogus tokens out
    card_holder, added_tokens = transpose_tokens(card_holder)

    # Add TCGPlayer information
    if not skip_tcgplayer:
        output_file["tcgplayerGroupId"] = tcgplayer.get_group_id(set_code.upper())
        card_holder = add_tcgplayer_ids(output_file["tcgplayerGroupId"], card_holder)

    output_file["totalSetSize"] = len(sf_cards)
    output_file["baseSetSize"] = output_file["totalSetSize"] - non_booster_cards
    output_file["cards"] = card_holder

    LOGGER.info("Finished cards for {}".format(set_code))

    LOGGER.info("Starting tokens for {}".format(set_code))
    sf_tokens: List[Dict[str, Any]] = scryfall.get_set("t" + set_code)
    output_file["tokens"] = build_mtgjson_tokens(sf_tokens + added_tokens)
    LOGGER.info("Finished tokens for {}".format(set_code))

    # Add UUID to each entry
    add_uuid_to_cards(output_file["cards"], output_file["tokens"], output_file)

    # Add Variations to each entry
    add_variations_field(output_file["cards"])

    if set_code[:2] == "DD":
        mark_duel_decks(output_file["cards"])

    return output_file


def add_uuid_to_cards(
    cards: List[Dict[str, Any]], tokens: List[Dict[str, Any]], file_info: Any
) -> None:
    """
    Each entry needs an ID. While we're really doing a hash,
    we will format it like a UUID for those who choose to
    consume in that format. Appends in-place to the arrays.
    :param cards: Cards Array
    :param tokens: Tokens Array
    :param file_info: <<CONST>> object for the file
    """
    # Only using attributes that _shouldn't_ change over time
    for card in cards:
        # Name + set code + colors (if applicable) + Scryfall UUID + printed text (if applicable)
        card_hash_code = (
            card["name"]
            + file_info["code"]
            + "".join(card.get("colors", ""))
            + card["scryfallId"]
            + str(card.get("originalText", ""))
        )

        card["uuid"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, card_hash_code))

    for token in tokens:
        # Name + set code + colors (if applicable) + power (if applicable) + toughness (if applicable) + Scryfall UUID
        token_hash_code = (
            token["name"]
            + "".join(token.get("colors", ""))
            + str(token.get("power", ""))
            + str(token.get("toughness", ""))
            + file_info["code"]
            + token["scryfallId"]
        )
        token["uuid"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, token_hash_code))


def transpose_tokens(
    cards: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Sometimes, tokens slip through and need to be transplanted
    back into their appropriate array. This method will allow
    us to pluck the tokens out and return them home.
    :param cards: Cards+Tokens to iterate
    :return: Cards, Tokens as two separate lists
    """
    # Order matters with these, as if you do cards first
    # it will shadow the tokens lookup

    # Single faced tokens are easy
    tokens = [
        scryfall.download(scryfall.SCRYFALL_API_CARD + card["scryfallId"])
        for card in cards
        if card["layout"] == "token"
    ]

    # Do not duplicate double faced tokens
    done_tokens: Set[str] = set()
    for card in cards:
        if (
            card["layout"] == "double_faced_token"
            and card["scryfallId"] not in done_tokens
        ):
            tokens.append(
                scryfall.download(scryfall.SCRYFALL_API_CARD + card["scryfallId"])
            )
            done_tokens.add(card["scryfallId"])

    # Remaining cards, without any kind of token
    cards = [
        card for card in cards if card["layout"] not in ["token", "double_faced_token"]
    ]

    return cards, tokens


def add_tcgplayer_ids(
    group_id: int, cards: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    For each card in the set, we will find its tcgplayer ID
    and add it to the card if found
    :param group_id: group to search for the cards
    :param cards: Cards list to add information to
    :return: Cards list with new information added
    """
    tcg_card_objs = tcgplayer.get_group_id_cards(group_id)

    for card in cards:
        prod_id = tcgplayer.get_card_id(card["name"], tcg_card_objs)
        if prod_id > 0:
            card["tcgplayerProductId"] = prod_id

    return cards


def uniquify_duplicates_in_set(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For cards with multiple printings in a set, we need to identify
    them against each other.
    For silver border sets, we will add (b), (c), ... to the end
    of the card name to do so.
    :param cards: Cards to check and update for repeats
    :return: updated cards list
    """
    if cards[0].get("borderColor", None) == "silver":
        unique_list = []
        duplicate_cards: Dict[str, int] = {}
        for card in cards:
            # Only if a card is duplicated in a set will it get the (a), (b) appended
            total_same_name_cards = sum(
                1 for item in cards if item["name"] == card["name"]
            )

            # Ignore basic lands
            if (card["name"] not in mtgjson4.BASIC_LANDS) and (
                card["name"] in duplicate_cards or total_same_name_cards > 1
            ):
                if card["name"] in duplicate_cards:
                    duplicate_cards[card["name"]] += 1
                else:
                    duplicate_cards[card["name"]] = ord("a")

                # Update the name of the card, and remove its names field (as it's not correct here)
                new_card = copy.deepcopy(card)
                # Only add (b), (c), ... so we have one unique without an altered name
                if chr(duplicate_cards[new_card["name"]]) != "a":
                    new_card["name"] += " ({0})".format(
                        chr(duplicate_cards[new_card["name"]])
                    )
                new_card.pop("names", None)
                unique_list.append(new_card)
            else:
                # Not a duplicate, just put the normal card into the list
                unique_list.append(card)

        return unique_list
    return cards


def add_variations_field(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For non-silver bordered sets, we will create a "variations"
    field will be created that has UUID of repeat cards
    :param cards: Cards to check and update for repeats
    :return: updated cards list
    """
    # Non-silver border sets use "variations"
    if cards[0].get("borderColor", None) != "silver":
        for card in cards:
            repeats_in_set = [
                item
                for item in cards
                if item["name"] == card["name"] and item["uuid"] != card["uuid"]
            ]

            variations = [r["uuid"] for r in repeats_in_set]
            if variations:
                card["variations"] = variations

    return cards


def add_start_flag_and_count_modified(
    set_code: str, search_url: str, mtgjson_cards: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Since SF doesn't provide individual card notices, we can post-process add the starter flag
    This method will also tell us how many starter cards are in the set
    :param set_code: Set to address
    :param search_url: URL to fix up to get non-booster cards
    :param mtgjson_cards: Modify the argument and return it
    :return: List of cards, number of cards modified
    """
    starter_card_url = search_url.replace("&unique=", "++not:booster&unique=")
    starter_cards = scryfall.download(starter_card_url)

    if starter_cards["object"] == "error":
        LOGGER.info("All cards in {} are available in boosters".format(set_code))
        return mtgjson_cards, 0

    for sf_card in starter_cards["data"]:
        # Each card has a unique UUID, even if they're the same card printed twice
        try:
            card = next(
                item for item in mtgjson_cards if item["scryfallId"] == sf_card["id"]
            )
            if card:
                card["starter"] = True
        except StopIteration:
            LOGGER.warning(
                "Passed on {0} with SF_ID {1}".format(
                    sf_card["name"], sf_card["scryfallId"]
                )
            )

    return mtgjson_cards, len(starter_cards["data"])


def build_mtgjson_tokens(
    sf_tokens: List[Dict[str, Any]], sf_card_face: int = 0
) -> List[Dict[str, Any]]:
    """
    Convert Scryfall tokens to MTGJSON tokens
    :param sf_tokens: All tokens in a set
    :param sf_card_face: Faces of the token index
    :return: List of MTGJSON tokens
    """
    token_cards: List[Dict[str, Any]] = []

    for sf_token in sf_tokens:
        mtgjson_card = {}
        if "card_faces" in sf_token:
            mtgjson_card["names"] = sf_token["name"].split(" // ")  # List[str]
            face_data = sf_token["card_faces"][sf_card_face]

            # Prevent duplicate UUIDs for split card halves
            # Remove the last character and replace with the id of the card face
            mtgjson_card["scryfallId"] = sf_token["id"] + str(sf_card_face)

            # Recursively parse the other cards within this card too
            # Only call recursive if it is the first time we see this card object
            if sf_card_face == 0:
                for i in range(1, len(sf_token["card_faces"])):
                    LOGGER.info(
                        "Parsing additional card {0} face {1}".format(
                            sf_token.get("name"), i
                        )
                    )
                    token_cards += build_mtgjson_tokens([sf_token], i)

            sf_token = face_data

        token_card: Dict[str, Any] = {}
        try:
            token_card = {
                "name": sf_token.get("name"),
                "type": sf_token.get("type_line"),
                "text": sf_token.get("oracle_text"),
                "power": sf_token.get("power"),
                "colors": sf_token.get("colors"),
                "colorIdentity": sf_token.get("color_identity"),
                "toughness": sf_token.get("toughness"),
                "loyalty": sf_token.get("loyalty"),
                "watermark": sf_token.get("watermark"),
                "scryfallId": sf_token["id"],
                "borderColor": sf_token.get("border_color"),
                "artist": sf_token.get("artist"),
                "isOnlineOnly": sf_token.get("digital"),
                "number": sf_token.get("collector_number"),
            }
        except KeyError:
            # Address duplicates, as only the original seems to have a UUID
            LOGGER.info(
                "Scryfall_ID not found in {}. Discarding {}".format(
                    sf_token.get("name"), sf_token
                )
            )
            continue

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
    # Clear sessions before the fork() to prevent awk issues with urllib3
    SESSION.set(None)

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
        "isOversized",
        "isOnlineOnly",
        "isTimeshifted",
        "isReserved",
        "frameEffect",
    ]

    for card_entry in card_list:
        insert_value = {}

        for key, value in card_entry.items():
            if value is not None:
                if (key in remove_field_if_false and value is False) or (value == ""):
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


def mark_duel_decks(cards: List[Dict[str, Any]]) -> None:
    """
    Duel decks are usually put together where the cards
    in the first deck are at the beginning, followed
    by basics, then start the second deck. We exploit
    this property to mark them as decks "a" and "b"
    :param cards: Cards in duel deck, sorted by number
    """
    basic_land_marked = False
    side_market = "a"

    for card in cards:
        if card["name"] in mtgjson4.BASIC_LANDS:
            basic_land_marked = True
        elif basic_land_marked:
            side_market = chr(ord(side_market) + 1)
            basic_land_marked = False

        card["duelDeck"] = side_market


def build_mtgjson_card(
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
        mtgjson_card["scryfallId"] = sf_card["id"]

        # Split cards and rotational cards have this field, flip cards do not.
        # Remove rotational cards via the additional check
        if "mana_cost" in sf_card and "//" in sf_card["mana_cost"]:
            mtgjson_card["colors"] = get_card_colors(
                sf_card["mana_cost"].split(" // ")[sf_card_face]
            )
            mtgjson_card["faceConvertedManaCost"] = get_cmc(
                sf_card["mana_cost"].split("//")[sf_card_face].strip()
            )
        elif sf_card["layout"] in ["split", "flip", "transform"]:
            # Handle non-normal cards, as they'll a face split
            mtgjson_card["faceConvertedManaCost"] = get_cmc(
                face_data.get("mana_cost", "0").strip()
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
        mtgjson_card["manaCost"] = face_data.get("mana_cost")

    if "colors" not in mtgjson_card:
        if "colors" in face_data:
            mtgjson_card["colors"] = face_data.get("colors")
        else:
            mtgjson_card["colors"] = sf_card.get("colors")

    mtgjson_card["name"] = face_data.get("name")
    mtgjson_card["type"] = face_data.get("type_line")
    mtgjson_card["text"] = face_data.get("oracle_text")

    mtgjson_card["power"] = face_data.get("power")
    mtgjson_card["toughness"] = face_data.get("toughness")
    mtgjson_card["loyalty"] = face_data.get("loyalty")
    mtgjson_card["watermark"] = face_data.get("watermark")

    if "flavor_text" in face_data:
        mtgjson_card["flavorText"] = face_data.get("flavor_text")
    else:
        mtgjson_card["flavorText"] = sf_card.get("flavor_text")

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

    mtgjson_card["frameVersion"] = sf_card.get("frame")
    mtgjson_card["hasFoil"] = sf_card.get("foil")
    mtgjson_card["hasNonFoil"] = sf_card.get("nonfoil")
    mtgjson_card["isOnlineOnly"] = sf_card.get("digital")
    mtgjson_card["isOversized"] = sf_card.get("oversized")
    mtgjson_card["layout"] = sf_card.get("layout")
    mtgjson_card["number"] = sf_card.get("collector_number")
    mtgjson_card["isReserved"] = sf_card.get("reserved")
    mtgjson_card["frameEffect"] = sf_card.get("frame_effect")

    # Add a "side" entry for split cards
    # Will only work for two faced cards (not meld, as they don't need this)
    if "names" in mtgjson_card and len(mtgjson_card["names"]) == 2:
        # chr(97) = 'a', chr(98) = 'b', ...
        mtgjson_card["side"] = chr(
            mtgjson_card["names"].index(mtgjson_card["name"]) + 97
        )

    if "scryfallId" not in mtgjson_card:
        mtgjson_card["scryfallId"] = sf_card.get("id")

    # Characteristics that we have to format ourselves from provided data
    mtgjson_card["isTimeshifted"] = (sf_card.get("frame") == "future") or (
        sf_card.get("set") == "tsb"
    )

    mtgjson_card["rarity"] = sf_card.get("rarity")
    if mtgjson_card.get("isTimeshifted", False):
        mtgjson_card["rarity"] = "timeshifted " + mtgjson_card["rarity"]

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

    # Handle meld and all parts tokens issues
    # Will re-address naming if a split card already
    if "all_parts" in sf_card:
        meld_holder = []
        mtgjson_card["names"] = []
        for a_part in sf_card["all_parts"]:
            if a_part["component"] != "token":
                if "//" in a_part.get("name"):
                    mtgjson_card["names"] = a_part.get("name").split(" // ")
                    break

                if "meld" in a_part["component"]:
                    meld_holder.append(a_part["component"])

                mtgjson_card["names"].append(a_part.get("name"))

        # If the only entry is the original card, empty the names array
        if (
            len(mtgjson_card["names"]) == 1
            and mtgjson_card["name"] in mtgjson_card["names"]
        ):
            del mtgjson_card["names"]

        # Meld cards should be CardA, Meld, CardB. This fixes that via swap
        if meld_holder and meld_holder[1] != "meld_result":
            mtgjson_card["names"][1], mtgjson_card["names"][2] = (
                mtgjson_card["names"][2],
                mtgjson_card["names"][1],
            )

    # Since we built meld cards later, we will add the "side" attribute now
    if len(mtgjson_card.get("names", [])) == 3:  # MELD
        if mtgjson_card["name"] == mtgjson_card["names"][0]:
            mtgjson_card["side"] = "a"
        elif mtgjson_card["name"] == mtgjson_card["names"][2]:
            mtgjson_card["side"] = "b"
        else:
            mtgjson_card["side"] = "c"

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
            LOGGER.warning(
                "Unable to parse originals for {}".format(mtgjson_card["name"])
            )

    mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards
