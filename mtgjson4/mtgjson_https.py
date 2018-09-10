import bs4
import configparser
import copy
import json
import logging
import mtgjson4
import multiprocessing
import pathlib
import requests
from typing import List, Dict, Any, Tuple, Set, Optional


def build_output_file(sf_cards: List[Dict[str, Any]], set_code: str) -> Dict[str, Any]:
    output_file: Dict[str, Any] = {}

    # Open set_outputs and read into config_file
    file_path: Optional[pathlib.Path] = find_file(f'{set_code.upper()}.json', mtgjson4.SET_CONFIG_DIR)
    if file_path:
        with pathlib.Path(file_path).open('r', encoding='utf-8') as f:
            config_file: Dict[str, Any] = json.loads(f.read())

            for key, value in config_file['SET'].items():
                output_file[key] = value
    else:
        logging.error("Set Config for {0} was not found, skipping...".format(set_code))
        return {"cards": []}

    # Declare the version of the build in the output file
    output_file['meta'] = {'version': mtgjson4.__VERSION__, 'date': mtgjson4.__VERSION_DATE__}

    logging.info("Starting cards for {}".format(set_code))
    output_file['cards'] = scryfall_to_mtgjson(sf_cards)
    logging.info("Finished cards for {}".format(set_code))

    tokens_dictionary = None  # TODO
    if tokens_dictionary:
        output_file['tokens'] = tokens_dictionary

    return output_file


def build_mtgjson_card(sf_card: Dict[str, Any], sf_card_face: int = 0, recurse: bool = True) -> List[Dict[str, Any]]:
    mtgjson_cards: List[Dict[str, Any]] = []
    mtgjson_card: Dict[str, Any] = {}

    # If flip-type, go to card_faces for alt attributes
    face_data: Dict[str, Any] = sf_card

    if "card_faces" in sf_card and recurse:
        mtgjson_card["names"] = sf_card["name"].split(" // ")  # List[str]
        face_data = sf_card["card_faces"][sf_card_face]

        # Recursively parse the other cards within this card too
        # Only call recursive if it is the first time we see this card object
        if sf_card_face == 0:
            for i in range(1, len(sf_card["card_faces"])):
                logging.info("Parsing additional card {0} face {1}".format(sf_card.get("name"), i))
                mtgjson_cards += build_mtgjson_card(sf_card, i, False)

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

    try:
        mtgjson_card["multiverseid"] = sf_card["multiverse_ids"][sf_card_face]  # int
    except IndexError:
        mtgjson_card["multiverseid"] = sf_card["multiverse_ids"][0]  # int

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
    mtgjson_card["foreignData"] = parse_sf_foreign(sf_card["prints_search_uri"], sf_card["set"])  # Dict[str, str]

    original_soup = download_from_gatherer(mtgjson_card["multiverseid"])
    div_name = layout_options(original_soup)

    mtgjson_card["originalText"] = parse_card_original_text(original_soup, div_name)  # str
    mtgjson_card["originalType"] = parse_card_original_type(original_soup, div_name)  # str

    logging.info("Parsed {0} from {1}".format(mtgjson_card.get("name"), sf_card.get("set")))
    mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards


def scryfall_to_mtgjson(sf_cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pool: multiprocessing.Pool = multiprocessing.Pool(processes=8)
    results = pool.map(build_mtgjson_card, sf_cards)
    return results


def download_from_scryfall(scryfall_url: str) -> Dict[str, Any]:
    """
    Get the data from Scryfall in JSON format using our secret keys
    :param scryfall_url: URL to download JSON data from
    :return: JSON object of the Scryfall data
    """
    # Open and read MTGJSON secret properties
    config = configparser.RawConfigParser()
    config.read(mtgjson4.CONFIG_PATH)

    request_api_json: Dict[str, Any] = requests.get(
        url=scryfall_url, headers={
            "Authorization": "Bearer " + config.get('Scryfall', 'client_secret')
        }).json()

    logging.info("Downloaded URL {0}".format(scryfall_url))
    return request_api_json


def get_scryfall_set(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    logging.info("Downloading set {0} information".format(set_code))
    set_api_json: Dict[str, Any] = download_from_scryfall(mtgjson4.SCRYFALL_API_SETS + set_code)
    cards_api_url: Optional[str] = set_api_json.get("search_uri")

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = list()

    # For each page, append all the data, go to next page
    page_downloaded: int = 1
    while cards_api_url is not None:
        logging.info("Downloading page {0} of card data for {1}".format(page_downloaded, set_code))
        page_downloaded += 1

        cards_api_json: Dict[str, Any] = download_from_scryfall(cards_api_url)

        for card in cards_api_json["data"]:
            scryfall_cards.append(card)

        if cards_api_json.get("has_more"):
            cards_api_url = cards_api_json.get("next_page")
        else:
            cards_api_url = None

    return scryfall_cards


def download_from_gatherer(card_mid: str) -> bs4.BeautifulSoup:
    request_data_html: Any = requests.get(
        url=mtgjson4.GATHERER_CARD, params={
            'multiverseid': str(card_mid),
            'printed': 'true',
            'page': '0'
        }, headers={})

    soup: bs4.BeautifulSoup = bs4.BeautifulSoup(request_data_html.text, 'html.parser')
    logging.info("Downloaded URL {0}".format(request_data_html.url))
    return soup


def parse_card_original_type(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    type_row = soup.find(id=parse_div.format('typeRow'))
    type_row = type_row.findAll('div')[-1]
    type_row = type_row.get_text(strip=True).replace('  ', ' ')
    return str(type_row)


def parse_card_original_text(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    text_row = soup.find(id=parse_div.format('textRow'))
    return_text = ''

    if text_row is not None:
        text_row = text_row.select('div[class^=cardtextbox]')

        return_text = ''
        for div in text_row:
            # Start by replacing all images with alternative text
            div = replace_images_with_text(div)
            # Next, just add the card text, line by line
            return_text += div.get_text() + '\n'
        return_text = return_text.strip()  # Remove last '\n'

    return return_text


def replace_images_with_text(tag: bs4.BeautifulSoup) -> bs4.BeautifulSoup:
    """
    Replaces the img tags of symbols with token representations
    :rtype: set
    :return: The color symbols found
    """
    tag_copy = copy.copy(tag)
    images = tag_copy.find_all('img')
    for symbol in images:
        symbol_value = symbol['alt']
        symbol_mapped = mtgjson4.get_symbol_short_name(symbol_value)
        symbol.replace_with(f'{{{symbol_mapped}}}')

    return tag_copy


def layout_options(soup: bs4.BeautifulSoup) -> str:
    number = soup.find_all('script')
    client_id_tags = ''
    for script in number:
        if 'ClientIDs' in script.get_text():
            client_id_tags = script.get_text()
            break

    # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{} for single cards or
    # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_ctl0*_{} for double cards, * being any int
    div_name = str((client_id_tags.split('ClientIDs.nameRow = \'')[1].split(';')[0])[:-8] + "{}").strip()
    return div_name


def parse_scryfall_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = download_from_scryfall(rulings_url)

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
        if value in mtgjson4.SUPERTYPES:
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


def parse_sf_foreign(sf_prints_url: str, set_name: str) -> List[Dict[str, str]]:
    card_foreign_entries: List[Dict[str, str]] = list()

    # Add information to get all languages
    sf_prints_url = sf_prints_url.replace("&unique=prints", "+lang%3Aany&unique=prints")

    prints_api_json: Dict[str, Any] = download_from_scryfall(sf_prints_url)
    for foreign_card in prints_api_json["data"]:
        if set_name != foreign_card["set"] or foreign_card["lang"] == "en":
            continue

        card_foreign_entry: Dict[str, str] = dict()
        card_foreign_entry["language"] = mtgjson4.LANGUAGE_MAP[foreign_card["lang"]]
        card_foreign_entry["multiverseid"] = foreign_card["multiverse_ids"][0]
        card_foreign_entry["text"] = foreign_card.get("printed_text")
        card_foreign_entry["flavor"] = foreign_card.get("flavor_text")
        card_foreign_entry["type"] = foreign_card.get("printed_type_line")

        card_foreign_entries.append(card_foreign_entry)

    return card_foreign_entries


def parse_scryfall_printings(sf_prints_url: str) -> List[str]:
    """
    Given a Scryfall printings URL, extract all sets a card was printed in
    :param sf_prints_url: URL to extract data from
    :return: List of all sets a specific card was printed in
    """
    card_sets: Set[str] = set()

    prints_api_json: Dict[str, Any] = download_from_scryfall(sf_prints_url)
    for card in prints_api_json["data"]:
        card_sets.add(card.get("set").upper())

    return list(card_sets)


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


def find_file(name: str, path: pathlib.Path) -> Optional[pathlib.Path]:
    """
    Function finds where on the path tree a specific file
    can be found. Useful for set_configs as we use sub
    directories to better organize data.
    """
    for file in path.glob('**/*.json'):
        if name == file.name:
            return file

    return None


def write_to_output(set_name: str, file_contents: Dict[str, Any]) -> None:
    """
    Write the compiled data to a file with the set's code
    Will ensure the output directory exists first
    """
    mtgjson4.COMPILED_OUTPUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(mtgjson4.COMPILED_OUTPUT_DIR, set_name.upper() + ".json").open('w', encoding='utf-8') as f:
        # file_contents["cards"] = remove_null_fields(file_contents["cards"])
        json.dump(file_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
        return


def main() -> None:
    """
    Temporary main method
    """
    set_list: List[str] = ["AKH", "ISD", "ORI"]
    scryfall_sets: List[List[Dict[str, Any]]] = [get_scryfall_set(set_code) for set_code in set_list]

    # For each set, build it in memory then dump it to a file
    # Prevents excessive memory usage
    for sf_set, set_code in zip(scryfall_sets, set_list):
        write_to_output(set_code, build_output_file(sf_set, set_code))


if __name__ == '__main__':
    main()
