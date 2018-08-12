import configparser
import json
import logging
import multiprocessing.pool
import pathlib
from typing import List, Dict, Any, Tuple, Set, Optional

import requests

import mtgjson41


def download_scryfall_json(scryfall_url: str) -> Dict[str, Any]:
    """
    Get the data from Scryfall in JSON format using our secret keys
    :param scryfall_url: URL to download JSON data from
    :return: JSON object of the Scryfall data
    """
    # Open and read MTGJSON secret properties
    config = configparser.RawConfigParser()
    config.read(mtgjson41.CONFIG_PATH)

    request_api_json: Dict[str, Any] = requests.get(
        url=scryfall_url, headers={
            "Authorization": "Bearer " + config.get('Scryfall', 'client_secret')
        }).json()

    logging.info("Downloaded URL {0}".format(scryfall_url))
    return request_api_json


def get_cards_from_scryfall(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    logging.info("Downloading set {0} information".format(set_code))
    set_api_json: Dict[str, Any] = download_scryfall_json(mtgjson41.SCRYFALL_API_SETS + set_code)
    cards_api_url: Optional[str] = set_api_json.get("search_uri")

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = list()

    # For each page, append all the data, go to next page
    page_downloaded: int = 1
    while cards_api_url is not None:
        logging.info("Downloading page {0} of card data for {1}".format(page_downloaded, set_code))
        page_downloaded += 1

        cards_api_json: Dict[str, Any] = download_scryfall_json(cards_api_url)

        for card in cards_api_json["data"]:
            scryfall_cards.append(card)

        if cards_api_json.get("has_more"):
            cards_api_url = cards_api_json.get("next_page")
        else:
            cards_api_url = None

    return scryfall_cards


def parse_scryfall_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = download_scryfall_json(rulings_url)

    sf_rules: List[Dict[str, str]] = list()
    mtgjson_rules: List[Dict[str, str]] = list()

    for rule in rules_api_json["data"]:
        sf_rules.append(rule)

    for sf_rule in sf_rules:
        mtgjson_rule: Dict[str, str] = dict()
        mtgjson_rule["date"] = sf_rule["published_at"]
        mtgjson_rule["text"] = sf_rule["comment"]
        mtgjson_rules.append(mtgjson_rule)

    return mtgjson_rules


def parse_scryfall_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Given a card type string, split it up into its raw components: super, sub, and type
    :param card_type: Card type string to parse
    :return: Tuple (super, type, sub) of the card's attributes
    """
    sub_types: List[str] = list()
    super_types: List[str] = list()
    types: List[str] = list()

    if '—' in card_type:
        split_type: List[str] = card_type.split('—')
        supertypes_and_types: str = split_type[0]
        subtypes: str = split_type[1]
        sub_types = [x for x in subtypes.split(' ') if x]
    else:
        supertypes_and_types = str(types)

    for value in supertypes_and_types.split(' '):
        if value in mtgjson41.SUPERTYPES:
            super_types.append(value)
        elif value:
            types.append(value)

    return super_types, types, sub_types


def parse_scryfall_legalities(sf_card_legalities: Dict[str, str]) -> Dict[str, str]:
    """
    Given a Scryfall legalities dictionary, convert it to MTGJSON format
    :param sf_card_legalities: Scryfall legalities
    :return: MTGJSON legalities
    """
    card_legalities: Dict[str, str] = dict()
    for key, value in sf_card_legalities.items():
        if value != "not_legal":
            card_legalities[key] = value.capitalize()

    return card_legalities


def parse_scryfall_foreign(sf_prints_url: str, set_name: str, mid_entry: int) -> List[Dict[str, str]]:
    card_foreign_entries: List[Dict[str, str]] = list()

    # Add information to get all languages
    sf_prints_url = sf_prints_url.replace("&unique=prints", "+lang%3Aany&unique=prints")

    prints_api_json: Dict[str, Any] = download_scryfall_json(sf_prints_url)
    for foreign_card in prints_api_json["data"]:
        if set_name != foreign_card["set"] or foreign_card["lang"] == "en":
            continue

        card_foreign_entry: Dict[str, str] = dict()
        card_foreign_entry["language"] = mtgjson41.LANGUAGE_MAP[foreign_card["lang"]]
        card_foreign_entry["multiverseid"] = foreign_card["multiverse_ids"][mid_entry]
        card_foreign_entry["text"] = foreign_card.get("printed_text")
        card_foreign_entry["flavor"] = foreign_card.get("flavor_text")
        card_foreign_entry["type"] = foreign_card["printed_type_line"]

        card_foreign_entries.append(card_foreign_entry)

    return card_foreign_entries


def parse_from_scryfall_cards(sf_cards: List[Dict[str, Any]], sf_card_face: int = 0) -> List[Dict[str, Any]]:
    mtgjson_cards: List[Dict[str, Any]] = list()

    for sf_card in sf_cards:
        mtgjson_card: Dict[str, Any] = dict()

        # If flip-type, go to card_faces for alt attributes
        face_data: Dict[str, Any] = sf_card
        if "card_faces" in sf_card:
            mtgjson_card["names"] = sf_card["name"].split(" // ")  # List[str]
            face_data = sf_card["card_faces"][sf_card_face]

            # Recursively parse the other cards within this card too
            # Only call recursive if it is the first time we see this card object
            if sf_card_face == 0:
                for i in range(1, len(sf_card["card_faces"])):
                    mtgjson_cards += parse_from_scryfall_cards([sf_card], i)

        # Characteristics that can are not shared to both sides of flip-type cards
        mtgjson_card["manaCost"] = face_data.get("mana_cost")  # str
        mtgjson_card["name"] = face_data.get("name")  # str
        mtgjson_card["type"] = face_data.get("type_line")  # str
        mtgjson_card["text"] = face_data.get("oracle_text")  # str
        mtgjson_card["colors"] = face_data.get("colors")  # List[str]
        mtgjson_card["power"] = face_data.get("power")  # str
        mtgjson_card["toughness"] = face_data.get("toughness")  # str
        mtgjson_card["loyalty"] = face_data.get("loyalty")  # str
        mtgjson_card["watermark"] = face_data.get("watermark")  # str
        mtgjson_card["multiverseid"] = sf_card["multiverse_ids"][sf_card_face]  # int

        # Characteristics that are shared to all sides of flip-type cards, that we don't have to modify
        mtgjson_card["artist"] = sf_card.get("artist")  # str
        mtgjson_card["borderColor"] = sf_card.get("border_color")
        mtgjson_card["colorIdentity"] = sf_card.get("color_identity")  # List[str]
        mtgjson_card["convertedManaCost"] = sf_card.get("cmc")  # float
        mtgjson_card["flavorText"] = sf_card.get("flavor_text")  # str
        mtgjson_card["frameVersion"] = sf_card.get("frame")  # str
        mtgjson_card["hasFoil"] = sf_card.get("foil")  # bool
        mtgjson_card["hasNonFoil"] = sf_card.get("nonfoil")  # bool
        mtgjson_card["isOnlineOnly"] = sf_card.get("digital")  # bool
        mtgjson_card["isOversized"] = sf_card.get("oversized")  # bool
        mtgjson_card["layout"] = sf_card.get("layout")  # str
        mtgjson_card["number"] = sf_card.get("collector_number")  # str
        mtgjson_card["reserved"] = sf_card.get("reserved")  # bool
        mtgjson_card["uuid"] = sf_card.get("id")  # str

        # Characteristics that we have to format ourselves from provided data
        mtgjson_card["timeshifted"] = (sf_card.get("timeshifted") or sf_card.get("futureshifted"))  # bool
        mtgjson_card["rarity"] = sf_card.get("rarity") if not mtgjson_card.get("timeshifted") else "Special"  # str

        # Characteristics that we need custom functions to parse
        mtgjson_card["legalities"] = parse_scryfall_legalities(sf_card["legalities"])  # Dict[str, str]
        mtgjson_card["rulings"] = parse_scryfall_rulings(sf_card["rulings_uri"])  # List[Dict[str, str]]
        mtgjson_card["printings"] = parse_scryfall_printings(sf_card["prints_search_uri"])  # List[str]

        card_types: Tuple[List[str], List[str], List[str]] = parse_scryfall_card_types(mtgjson_card["type"])
        mtgjson_card["supertypes"] = card_types[0]  # List[str]
        mtgjson_card["types"] = card_types[1]  # List[str]
        mtgjson_card["subtypes"] = card_types[2]  # List[str]

        # Characteristics that we cannot get from Scryfall
        # Characteristics we have to do further API calls for
        mtgjson_card["foreignData"] = parse_scryfall_foreign(sf_card["prints_search_uri"], sf_card["set"], sf_card_face)
        mtgjson_card["originalText"] = ""
        mtgjson_card["originalType"] = ""

        logging.info("Parsed {0} from {1}".format(mtgjson_card.get("name"), sf_card.get("set")))
        mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards


def parse_scryfall_printings(sf_prints_url: str) -> List[str]:
    """
    Given a Scryfall printings URL, extract all sets a card was printed in
    :param sf_prints_url: URL to extract data from
    :return: List of all sets a specific card was printed in
    """
    card_sets: Set[str] = set()

    prints_api_json: Dict[str, Any] = download_scryfall_json(sf_prints_url)
    for card in prints_api_json["data"]:
        card_sets.add(card.get("set").upper())

    return list(card_sets)


def write_to_output(set_name: str, file_contents: List[Dict[str, Any]]) -> None:
    """
    Write the compiled data to a file with the set's code
    Will ensure the output directory exists first
    """
    mtgjson41.COMPILED_OUTPUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(mtgjson41.COMPILED_OUTPUT_DIR, set_name.upper() + ".json").open('w', encoding='utf-8') as f:
        new_contents: List[Dict[str, Any]] = remove_null_fields(file_contents)
        json.dump(new_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
        return


def remove_null_fields(card_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Recursively remove all null values found
    """
    fixed_dict: List[Dict[str, Any]] = list()

    for card_entry in card_list:
        insert_value = dict()
        for key, value in card_entry.items():
            if value is not None:
                insert_value[key] = value

        fixed_dict.append(insert_value)

    return fixed_dict


def main() -> None:
    """
    Temporary main method
    """
    set_list: List[str] = ["w16"]
    scryfall_sets: List[List[Dict[str, Any]]] = [get_cards_from_scryfall(set_code) for set_code in set_list]
    data_bank: List[List[Dict[str, Any]]] = list()

    pool: multiprocessing.pool.ThreadPool = multiprocessing.pool.ThreadPool()
    for sf_set in scryfall_sets:
        data_bank.append(pool.apply(parse_from_scryfall_cards, args=(
            sf_set,
            0,
        )))
    pool.close()

    for set_name, data in zip(set_list, data_bank):
        write_to_output(set_name, data)


if __name__ == '__main__':
    main()
