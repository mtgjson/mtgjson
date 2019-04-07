"""Compile incoming data into the target output format."""
import contextvars
import copy
import json
import logging
import multiprocessing
import pathlib
import re
from typing import Any, Dict, List, Set, Tuple

import mtgjson4
from mtgjson4 import mtgjson_card
from mtgjson4.mtgjson_card import MTGJSONCard
from mtgjson4.provider import gatherer, scryfall, tcgplayer, wizards
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

    # Get the set config from Scryfall
    set_config = scryfall.download(scryfall.SCRYFALL_API_SETS + set_code)
    if set_config["object"] == "error":
        LOGGER.error("Set Config for {} was not found, skipping...".format(set_code))
        return {"cards": [], "tokens": []}

    output_file["name"] = set_config["name"]
    output_file["code"] = set_config["code"].upper()
    output_file["releaseDate"] = set_config["released_at"]
    output_file["type"] = set_config["set_type"]
    output_file["keyruneCode"] = (
        pathlib.Path(set_config["icon_svg_uri"]).name.split(".")[0].upper()
    )

    # Add translations to the files
    try:
        output_file["translations"] = wizards.get_translations(output_file["name"])
    except KeyError:
        LOGGER.warning("Unable to find set translations for {}".format(set_code))

    # Add Card Market information, if it exists
    with mtgjson4.RESOURCE_PATH.joinpath("mkm_information.json").open("r") as f:
        mcm_json = json.load(f)
        if output_file["code"] in mcm_json.keys():
            output_file["mcmName"] = mcm_json[output_file["code"]]["mcmName"]
            output_file["mcmId"] = mcm_json[output_file["code"]]["mcmId"]

    # Add optionals if they exist
    if "mtgo_code" in set_config.keys():
        output_file["mtgoCode"] = set_config["mtgo_code"].upper()

    if "parent_set_code" in set_config.keys():
        output_file["parentCode"] = set_config["parent_set_code"].upper()

    if "block" in set_config.keys():
        output_file["block"] = set_config["block"]

    if "digital" in set_config.keys():
        output_file["isOnlineOnly"] = set_config["digital"]

    if "foil_only" in set_config.keys():
        output_file["isFoilOnly"] = set_config["foil_only"]

    # Add booster info based on boosters resource (manually maintained for the time being)
    with mtgjson4.RESOURCE_PATH.joinpath("boosters.json").open(
        "r", encoding="utf-8"
    ) as f:
        json_dict: Dict[str, List[Any]] = json.load(f)
        if output_file["code"] in json_dict.keys():
            output_file["boosterV3"] = json_dict[output_file["code"].upper()]

    # Add V3 code for some backwards compatibility
    with mtgjson4.RESOURCE_PATH.joinpath("gatherer_set_codes.json").open(
        "r", encoding="utf-8"
    ) as f:
        json_dict = json.load(f)
        if output_file["code"] in json_dict.keys():
            output_file["codeV3"] = json_dict[output_file["code"]]

    # Declare the version of the build in the output file
    output_file["meta"] = {
        "version": mtgjson4.__VERSION__,
        "date": mtgjson4.__VERSION_DATE__,
    }

    LOGGER.info("Starting cards for {}".format(set_code))

    card_holder: List[MTGJSONCard] = convert_to_mtgjson(sf_cards)
    card_holder = add_start_flag_and_count_modified(
        set_code, set_config["search_uri"], card_holder
    )

    # Address duplicates in un-sets
    card_holder = uniquify_duplicates_in_set(card_holder)

    # Move bogus tokens out
    card_holder, added_tokens = transpose_tokens(card_holder)

    # Add TCGPlayer information
    if "tcgplayer_id" in set_config.keys():
        output_file["tcgplayerGroupId"] = set_config["tcgplayer_id"]
        if not skip_tcgplayer:
            add_tcgplayer_fields(output_file["tcgplayerGroupId"], card_holder)

    # Set sizes; BASE SET SIZE WILL BE UPDATED BELOW
    output_file["totalSetSize"] = len(sf_cards)

    with mtgjson4.RESOURCE_PATH.joinpath("base_set_sizes.json").open(
        "r", encoding="utf-8"
    ) as f:
        # Use the value in the resources file, otherwise use total set size
        output_file["baseSetSize"] = json.load(f).get(
            set_code.upper(), output_file["totalSetSize"]
        )

    output_file["cards"] = card_holder

    LOGGER.info("Finished cards for {}".format(set_code))

    # Handle tokens
    LOGGER.info("Starting tokens for {}".format(set_code))
    sf_tokens: List[Dict[str, Any]] = scryfall.get_set("t" + set_code)
    output_file["tokens"] = build_mtgjson_tokens(sf_tokens + added_tokens)
    LOGGER.info("Finished tokens for {}".format(set_code))

    # Cleanups and UUIDs
    mtgjson_card.DUEL_DECK_LAND_MARKED.set(False)
    mtgjson_card.DUEL_DECK_SIDE_COMP.set("a")
    for card in output_file["cards"]:
        card.final_card_cleanup()
    for token in output_file["tokens"]:
        token.final_card_cleanup(is_card=False)

    # Add Variations to each entry, as well as mark alternatives
    add_variations_and_alternative_fields(output_file["cards"], output_file)

    return output_file


def transpose_tokens(
    cards: List[MTGJSONCard]
) -> Tuple[List[MTGJSONCard], List[Dict[str, Any]]]:
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
        scryfall.download(scryfall.SCRYFALL_API_CARD + card.get("scryfallId"))
        for card in cards
        if card.get("layout") == "token"
    ]

    # Do not duplicate double faced tokens
    done_tokens: Set[str] = set()
    for card in cards:
        if (
            card.get("layout") == "double_faced_token"
            and card.get("scryfallId") not in done_tokens
        ):
            tokens.append(
                scryfall.download(scryfall.SCRYFALL_API_CARD + card.get("scryfallId"))
            )
            done_tokens.add(card.get("scryfallId"))

    # Remaining cards, without any kind of token
    cards = [
        card
        for card in cards
        if card.get("layout") not in ["token", "double_faced_token"]
    ]

    return cards, tokens


def add_tcgplayer_fields(group_id: int, cards: List[MTGJSONCard]) -> None:
    """
    For each card in the set, we will find its tcgplayer ID
    and add it to the card if found
    :param group_id: group to search for the cards
    :param cards: Cards list to add information to
    """
    tcg_card_objs = tcgplayer.get_group_id_cards(group_id)
    for card in cards:
        card.add_tcgplayer_fields(tcg_card_objs)


def uniquify_duplicates_in_set(cards: List[MTGJSONCard]) -> List[MTGJSONCard]:
    """
    For cards with multiple printings in a set, we need to identify
    them against each other.
    For silver border sets, we will add (b), (c), ... to the end
    of the card name to do so.
    :param cards: Cards to check and update for repeats
    :return: updated cards list
    """
    if cards and cards[0].get("borderColor", None) == "silver":
        unique_list = []
        duplicate_cards: Dict[str, int] = {}
        for card in cards:
            # Only if a card is duplicated in a set will it get the (a), (b) appended
            total_same_name_cards = sum(
                1 for item in cards if item.get("name") == card.get("name")
            )

            # Ignore basic lands
            if (card.get("name") not in mtgjson4.BASIC_LANDS) and (
                card.get("name") in duplicate_cards or total_same_name_cards > 1
            ):
                if card.get("name") in duplicate_cards:
                    duplicate_cards[card.get("name")] += 1
                else:
                    duplicate_cards[card.get("name")] = ord("a")

                # Update the name of the card, and remove its names field (as it's not correct here)
                new_card = copy.deepcopy(card)
                # Only add (b), (c), ... so we have one unique without an altered name
                if chr(duplicate_cards[new_card.get("name")]) != "a":
                    new_card.append(
                        "name",
                        " ({0})".format(chr(duplicate_cards[new_card.get("name")])),
                    )
                new_card.remove("names")
                unique_list.append(new_card)
            else:
                # Not a duplicate, just put the normal card into the list
                unique_list.append(card)

        return unique_list
    return cards


def add_variations_and_alternative_fields(
    cards: List[MTGJSONCard], file_info: Any
) -> None:
    """
    For non-silver bordered sets, we will create a "variations"
    field will be created that has UUID of repeat cards.
    This will also mark alternative printings within a single set.
    :param cards: Cards to check and update for repeats
    :param file_info: <<CONST>> object for the file
    :return: How many alternative printings were marked
    """
    # Non-silver border sets use "variations"
    if cards and cards[0].get("borderColor") != "silver":
        for card in cards:
            repeats_in_set = [
                item
                for item in cards
                if item.get("name") == card.get("name")
                and item.get("uuid") != card.get("uuid")
            ]

            # Add variations field
            variations = [r.get("uuid") for r in repeats_in_set]
            if variations:
                card.set("variations", variations)

            # Add alternative tag
            # Ignore singleton printings in set, as well as basics
            if not repeats_in_set or card.get("name") in mtgjson4.BASIC_LANDS:
                continue

            # Some hardcoded checking due to inconsistencies upstream
            if file_info["code"].upper() in ["UNH", "10E"]:
                # Check for duplicates, mark the foils
                if (
                    len(repeats_in_set) >= 1
                    and card.get("hasFoil")
                    and not card.get("hasNonFoil")
                ):
                    card.set("isAlternative", True)
            elif file_info["code"].upper() in ["CN2", "BBD"]:
                # Check for set number > set size
                if (
                    int(card.get("number").replace(chr(9733), ""))
                    > file_info["baseSetSize"]
                ):
                    card.set("isAlternative", True)
            elif file_info["code"].upper() == "PLS":
                # Check for a star in the number
                if chr(9733) in card.get("number"):
                    card.set("isAlternative", True)


def add_start_flag_and_count_modified(
    set_code: str, search_url: str, mtgjson_cards: List[MTGJSONCard]
) -> List[MTGJSONCard]:
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
        return mtgjson_cards

    for sf_card in starter_cards["data"]:
        # Each card has a unique UUID, even if they're the same card printed twice
        try:
            card = next(
                item
                for item in mtgjson_cards
                if item.get("scryfallId") == sf_card["id"]
            )
            if card:
                card.set("isStarter", True)
        except StopIteration:
            LOGGER.warning(
                "Passed on {0} with SF_ID {1}".format(
                    sf_card["name"], sf_card["scryfallId"]
                )
            )

    return mtgjson_cards


def build_mtgjson_tokens(
    sf_tokens: List[Dict[str, Any]], sf_card_face: int = 0
) -> List[MTGJSONCard]:
    """
    Convert Scryfall tokens to MTGJSON tokens
    :param sf_tokens: All tokens in a set
    :param sf_card_face: Faces of the token index
    :return: List of MTGJSON tokens
    """
    token_cards: List[MTGJSONCard] = []

    for sf_token in sf_tokens:
        token_card = MTGJSONCard(sf_token["set"])

        if "card_faces" in sf_token:
            token_card.set("names", sf_token["name"].split(" // "))
            face_data = sf_token["card_faces"][sf_card_face]

            # Prevent duplicate UUIDs for split card halves
            # Remove the last character and replace with the id of the card face
            token_card.set("scryfallId", sf_token["id"])
            token_card.set("scryfallOracleId", sf_token["oracle_id"])
            token_card.set("scryfallIllustrationId", sf_token.get("illustration_id"))

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

            if "id" not in sf_token.keys():
                LOGGER.info(
                    "Scryfall_ID not found in {}. Discarding {}".format(
                        sf_token.get("name"), sf_token
                    )
                )
                continue

            token_card.set_all(
                {
                    "name": face_data.get("name"),
                    "type": face_data.get("type_line"),
                    "text": face_data.get("oracle_text"),
                    "power": face_data.get("power"),
                    "colors": face_data.get("colors"),
                    "colorIdentity": sf_token.get("color_identity"),
                    "toughness": face_data.get("toughness"),
                    "loyalty": face_data.get("loyalty"),
                    "watermark": sf_token.get("watermark"),
                    "scryfallId": sf_token["id"],
                    "scryfallOracleId": sf_token.get("oracle_id"),
                    "scryfallIllustrationId": sf_token.get("illustration_id"),
                    "layout": "double_faced_token",
                    "side": chr(97 + sf_card_face),
                    "borderColor": face_data.get("border_color"),
                    "artist": face_data.get("artist"),
                    "isOnlineOnly": sf_token.get("digital"),
                    "number": sf_token.get("collector_number"),
                }
            )
        else:
            token_card.set_all(
                {
                    "name": sf_token.get("name"),
                    "type": sf_token.get("type_line"),
                    "text": sf_token.get("oracle_text"),
                    "power": sf_token.get("power"),
                    "colors": sf_token.get("colors"),
                    "colorIdentity": sf_token.get("color_identity"),
                    "toughness": sf_token.get("toughness"),
                    "loyalty": sf_token.get("loyalty"),
                    "layout": "normal",
                    "watermark": sf_token.get("watermark"),
                    "scryfallId": sf_token["id"],
                    "scryfallOracleId": sf_token.get("oracle_id"),
                    "scryfallIllustrationId": sf_token.get("illustration_id"),
                    "borderColor": sf_token.get("border_color"),
                    "artist": sf_token.get("artist"),
                    "isOnlineOnly": sf_token.get("digital"),
                    "number": sf_token.get("collector_number"),
                }
            )

        reverse_related: List[str] = []
        if "all_parts" in sf_token:
            for a_part in sf_token["all_parts"]:
                if a_part.get("name") != token_card.get("name"):
                    reverse_related.append(a_part.get("name"))
        token_card.set("reverseRelated", reverse_related)

        LOGGER.info(
            "Parsed {0} from {1}".format(token_card.get("name"), sf_token.get("set"))
        )
        token_cards.append(token_card)

    return token_cards


def convert_to_mtgjson(sf_cards: List[Dict[str, Any]]) -> List[MTGJSONCard]:
    """
    Parallel method to build each card in the set
    :param sf_cards: cards to build
    :return: list of cards built
    """
    # Clear sessions before the fork() to prevent awk issues with urllib3
    SESSION.set(None)

    with multiprocessing.Pool(processes=8) as pool:
        results: List[Any] = pool.map(build_mtgjson_card, sf_cards)

        all_cards: List[MTGJSONCard] = []
        for cards in results:
            for card in cards:
                all_cards.append(card)
    return all_cards


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


def build_mtgjson_card(
    sf_card: Dict[str, Any], sf_card_face: int = 0
) -> List[MTGJSONCard]:
    """
    Build a mtgjson card (and all sub pieces of that card)
    :param sf_card: Card to build
    :param sf_card_face: Which part of the card (defaults to 0)
    :return: List of card(s) build (usually 1)
    """
    mtgjson_cards: List[MTGJSONCard] = []
    single_card = MTGJSONCard(sf_card["set"])

    # Let us know what card we're trying to parse -- good for debugging :)
    LOGGER.info("Parsing {0} from {1}".format(sf_card.get("name"), sf_card.get("set")))

    # If flip-type, go to card_faces for alt attributes
    face_data: Dict[str, Any] = sf_card

    if "card_faces" in sf_card:
        single_card.set_all(
            {
                "names": sf_card["name"].split(" // "),
                "scryfallId": sf_card["id"],
                "scryfallOracleId": sf_card["oracle_id"],
                "scryfallIllustrationId": sf_card.get("illustration_id"),
            }
        )
        face_data = sf_card["card_faces"][sf_card_face]

        # Split cards and rotational cards have this field, flip cards do not.
        # Remove rotational cards via the additional check
        if "mana_cost" in sf_card and "//" in sf_card["mana_cost"]:
            single_card.set(
                "colors",
                get_card_colors(sf_card["mana_cost"].split(" // ")[sf_card_face]),
            )
            single_card.set(
                "faceConvertedManaCost",
                get_cmc(sf_card["mana_cost"].split("//")[sf_card_face].strip()),
            )
        elif sf_card["layout"] in ["split", "flip", "transform"]:
            # Handle non-normal cards, as they'll a face split
            single_card.set(
                "faceConvertedManaCost",
                get_cmc(face_data.get("mana_cost", "0").strip()),
            )

        # Watermark is only attributed on the front side, so we'll account for it
        single_card.set(
            "watermark",
            sf_card["card_faces"][0].get("watermark", None),
            single_card.clean_up_watermark,
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
    else:
        single_card.set_all(
            {
                "scryfallId": sf_card.get("id"),
                "scryfallOracleId": sf_card["oracle_id"],
                "scryfallIllustrationId": sf_card.get("illustration_id"),
            }
        )

    # Characteristics that can are not shared to both sides of flip-type cards
    if face_data.get("mana_cost"):
        single_card.set("manaCost", face_data.get("mana_cost"))

    if "colors" not in single_card.keys():
        if "colors" in face_data:
            single_card.set("colors", face_data.get("colors"))
        else:
            single_card.set("colors", sf_card.get("colors"))

    single_card.set_all(
        {
            "name": face_data.get("name"),
            "type": face_data.get("type_line"),
            "text": face_data.get("oracle_text"),
            "power": face_data.get("power"),
            "toughness": face_data.get("toughness"),
            "loyalty": face_data.get("loyalty"),
            "artist": sf_card.get("artist"),
            "borderColor": sf_card.get("border_color"),
            "colorIdentity": sf_card.get("color_identity"),
            "frameVersion": sf_card.get("frame"),
            "hasFoil": sf_card.get("foil"),
            "hasNonFoil": sf_card.get("nonfoil"),
            "isOnlineOnly": sf_card.get("digital"),
            "isOversized": sf_card.get("oversized"),
            "layout": sf_card.get("layout"),
            "number": sf_card.get("collector_number"),
            "isReserved": sf_card.get("reserved"),
            "frameEffect": sf_card.get("frame_effect"),
            "tcgplayerProductId": sf_card.get("tcgplayer_id"),
            "life": sf_card.get("life_modifier"),
            "hand": sf_card.get("hand_modifier"),
            "convertedManaCost": sf_card.get("cmc"),
        }
    )

    if "watermark" not in single_card.keys():
        single_card.set(
            "watermark",
            face_data.get("watermark", None),
            single_card.clean_up_watermark,
        )

    if "flavor_text" in face_data:
        single_card.set("flavorText", face_data.get("flavor_text"))
    else:
        single_card.set("flavorText", sf_card.get("flavor_text"))

    if "color_indicator" in face_data:
        single_card.set("colorIndicator", face_data.get("color_indicator"))
    elif "color_indicator" in sf_card:
        single_card.set("colorIndicator", sf_card.get("color_indicator"))

    try:
        single_card.set("multiverseId", sf_card["multiverse_ids"][sf_card_face])
    except IndexError:
        try:
            single_card.set("multiverseId", sf_card["multiverse_ids"][0])
        except IndexError:
            single_card.set("multiverseId", None)

    # Add a "side" entry for split cards
    # Will only work for two faced cards (not meld, as they don't need this)
    if "names" in single_card.keys() and single_card.names_count(2):
        # chr(97) = 'a', chr(98) = 'b', ...
        single_card.set(
            "side", chr(single_card.get("names").index(single_card.get("name")) + 97)
        )

    # Characteristics that we have to format ourselves from provided data
    single_card.set(
        "isTimeshifted",
        (sf_card.get("frame") == "future") or (sf_card.get("set") == "tsb"),
    )

    single_card.set("rarity", sf_card.get("rarity"))

    # Characteristics that we need custom functions to parse
    print_search_url: str = sf_card["prints_search_uri"].replace("%22", "")
    single_card.set("legalities", scryfall.parse_legalities(sf_card["legalities"]))
    single_card.set(
        "rulings",
        sorted(
            scryfall.parse_rulings(sf_card["rulings_uri"]),
            key=lambda ruling: ruling["date"],
        ),
    )
    single_card.set("printings", sorted(scryfall.parse_printings(print_search_url)))

    card_types: Tuple[List[str], List[str], List[str]] = scryfall.parse_card_types(
        single_card.get("type")
    )
    single_card.set("supertypes", card_types[0])
    single_card.set("types", card_types[1])
    single_card.set("subtypes", card_types[2])

    # Handle meld and all parts tokens issues
    # Will re-address naming if a split card already
    if "all_parts" in sf_card:
        meld_holder = []
        single_card.set("names", [])
        for a_part in sf_card["all_parts"]:
            if a_part["component"] != "token":
                if "//" in a_part.get("name"):
                    single_card.set("names", a_part.get("name").split(" // "))
                    break

                # This is a meld only-fix, so we ignore tokens/combo pieces
                if "meld" in a_part["component"]:
                    meld_holder.append(a_part["component"])

                    single_card.append("names", a_part.get("name"))

        # If the only entry is the original card, empty the names array
        if single_card.names_count(1) and single_card.get("name") in single_card.get(
            "names"
        ):
            single_card.remove("names")

        # Meld cards should be CardA, Meld, CardB. This fixes that via swap
        # meld_holder

        if meld_holder and meld_holder[1] != "meld_result":
            single_card.get("names")[1], single_card.get("names")[2] = (
                single_card.get("names")[2],
                single_card.get("names")[1],
            )

    # Since we built meld cards later, we will add the "side" attribute now
    if single_card.names_count(3):  # MELD
        if single_card.get("name") == single_card.get("names")[0]:
            single_card.set("side", "a")
        elif single_card.get("name") == single_card.get("names")[2]:
            single_card.set("side", "b")
        else:
            single_card.set("side", "c")

    # Characteristics that we cannot get from Scryfall
    # Characteristics we have to do further API calls for
    single_card.set(
        "foreignData",
        scryfall.parse_foreign(
            print_search_url,
            single_card.get("name"),
            single_card.get("number"),
            sf_card["set"],
        ),
    )

    if single_card.get("multiverseId") is not None:
        gatherer_cards = gatherer.get_cards(single_card.get("multiverseId"))
        try:
            gatherer_card = gatherer_cards[sf_card_face]
            single_card.set("originalType", gatherer_card.original_types)
            single_card.set("originalText", gatherer_card.original_text)
        except IndexError:
            LOGGER.warning(
                "Unable to parse originals for {}".format(single_card.get("name"))
            )

    mtgjson_cards.append(single_card)
    return mtgjson_cards
