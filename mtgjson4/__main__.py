"""
MTGJSON Version 4 Compiler
"""
import argparse
import configparser
import copy
import json
import multiprocessing
import pathlib
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import bs4
import requests

import mtgjson4


def build_output_file(sf_cards: List[Dict[str, Any]], set_code: str) -> Dict[str, Any]:
    """
    Compile the entire XYZ.json file and pass it off to be written out
    :param sf_cards: Scryfall cards
    :param set_code: Set code
    :return: Completed JSON file
    """
    output_file: Dict[str, Any] = {}

    # Get the set config from ScryFall
    set_config = download_from_scryfall(mtgjson4.SCRYFALL_API_SETS + set_code)
    if set_config['object'] == 'error':
        mtgjson4.LOGGER.error('Set Config for {} was not found, skipping...'.format(set_code))
        return {'cards': []}

    output_file['name'] = set_config.get('name')
    output_file['code'] = set_config.get('code')
    output_file['mtgoCode'] = set_config.get('mtgo_code')
    output_file['releaseDate'] = set_config.get('released_at')
    output_file['type'] = set_config.get('set_type')
    # output_file['booster'] = ''  # Maybe will re-add

    if set_config.get('digital'):
        output_file['onlineOnly'] = True

    if set_config.get('foil_only'):
        output_file['foilOnly'] = True

    # Declare the version of the build in the output file
    output_file['meta'] = {'version': mtgjson4.__VERSION__, 'date': mtgjson4.__VERSION_DATE__}

    mtgjson4.LOGGER.info('Starting cards for {}'.format(set_code))
    output_file['cards'] = scryfall_to_mtgjson(sf_cards)
    mtgjson4.LOGGER.info('Finished cards for {}'.format(set_code))

    mtgjson4.LOGGER.info('Starting tokens for {}'.format(set_code))
    sf_tokens: List[Dict[str, Any]] = get_scryfall_set('t' + set_code)
    output_file['tokens'] = build_mtgjson_tokens(sf_tokens)
    mtgjson4.LOGGER.info('Finished tokens for {}'.format(set_code))

    return output_file


def build_mtgjson_tokens(sf_tokens: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert Scryfall tokens to MTGJSON tokens
    :param sf_tokens: All tokens in a set
    :return: List of MTGJSON tokens
    """
    token_cards: List[Dict[str, Any]] = []

    for sf_token in sf_tokens:
        token_card: Dict[str, Any] = {
            'name': sf_token.get('name'),
            'type': sf_token.get('type_line'),
            'text': sf_token.get('oracle_text'),
            'power': sf_token.get('power'),
            'colors': sf_token.get('colors'),
            'colorIdentity': sf_token.get('color_identity'),
            'toughness': sf_token.get('toughness'),
            'loyalty': sf_token.get('loyalty'),
            'watermark': sf_token.get('watermark'),
            'uuid': sf_token.get('id'),
            'borderColor': sf_token.get('border_color'),
            'artist': sf_token.get('artist'),
            'isOnlineOnly': sf_token.get('digital'),
            'number': sf_token.get('collector_number')
        }

        reverse_related: List[str] = []
        if 'all_parts' in sf_token:
            for a_part in sf_token['all_parts']:
                if a_part.get('name') != token_card.get('name'):
                    reverse_related.append(a_part.get('name'))
        token_card['reverseRelated'] = reverse_related

        mtgjson4.LOGGER.info('Parsed {0} from {1}'.format(token_card.get('name'), sf_token.get('set')))
        token_cards.append(token_card)

    return token_cards


def build_mtgjson_card(sf_card: Dict[str, Any], sf_card_face: int = 0) -> List[Dict[str, Any]]:
    """
    Build a mtgjson card (and all sub pieces of that card)
    :param sf_card: Card to build
    :param sf_card_face: Which part of the card (defaults to 0)
    :return: List of card(s) build (usually 1)
    """
    mtgjson_cards: List[Dict[str, Any]] = []
    mtgjson_card: Dict[str, Any] = {}

    # If flip-type, go to card_faces for alt attributes
    face_data: Dict[str, Any] = sf_card

    if 'card_faces' in sf_card:
        mtgjson_card['names'] = sf_card['name'].split(' // ')  # List[str]
        face_data = sf_card['card_faces'][sf_card_face]

        # Recursively parse the other cards within this card too
        # Only call recursive if it is the first time we see this card object
        if sf_card_face == 0:
            for i in range(1, len(sf_card['card_faces'])):
                mtgjson4.LOGGER.info('Parsing additional card {0} face {1}'.format(sf_card.get('name'), i))
                mtgjson_cards += build_mtgjson_card(sf_card, i)

    # Characteristics that can are not shared to both sides of flip-type cards
    mtgjson_card['manaCost'] = face_data.get('mana_cost')  # str
    mtgjson_card['name'] = face_data.get('name')  # str
    mtgjson_card['type'] = face_data.get('type_line')  # str
    mtgjson_card['text'] = face_data.get('oracle_text')  # str
    mtgjson_card['colors'] = face_data.get('colors')  # List[str]
    mtgjson_card['power'] = face_data.get('power')  # str
    mtgjson_card['toughness'] = face_data.get('toughness')  # str
    mtgjson_card['loyalty'] = face_data.get('loyalty')  # str
    mtgjson_card['watermark'] = face_data.get('watermark')  # str

    try:
        mtgjson_card['multiverseid'] = sf_card['multiverse_ids'][sf_card_face]  # int
    except IndexError:
        try:
            mtgjson_card['multiverseid'] = sf_card['multiverse_ids'][0]  # int
        except IndexError:
            mtgjson_card['multiverseid'] = None  # int

    # Characteristics that are shared to all sides of flip-type cards, that we don't have to modify
    mtgjson_card['artist'] = sf_card.get('artist')  # str
    mtgjson_card['borderColor'] = sf_card.get('border_color')
    mtgjson_card['colorIdentity'] = sf_card.get('color_identity')  # List[str]
    mtgjson_card['convertedManaCost'] = sf_card.get('cmc')  # float
    mtgjson_card['flavorText'] = sf_card.get('flavor_text')  # str
    mtgjson_card['frameVersion'] = sf_card.get('frame')  # str
    mtgjson_card['hasFoil'] = sf_card.get('foil')  # bool
    mtgjson_card['hasNonFoil'] = sf_card.get('nonfoil')  # bool
    mtgjson_card['isOnlineOnly'] = sf_card.get('digital')  # bool
    mtgjson_card['isOversized'] = sf_card.get('oversized')  # bool
    mtgjson_card['layout'] = sf_card.get('layout')  # str
    mtgjson_card['number'] = sf_card.get('collector_number')  # str
    mtgjson_card['reserved'] = sf_card.get('reserved')  # bool
    mtgjson_card['uuid'] = sf_card.get('id')  # str

    # Characteristics that we have to format ourselves from provided data
    mtgjson_card['timeshifted'] = (sf_card.get('timeshifted') or sf_card.get('futureshifted'))  # bool
    mtgjson_card['rarity'] = sf_card.get('rarity') if not mtgjson_card.get('timeshifted') else 'Special'  # str

    # Characteristics that we need custom functions to parse
    mtgjson_card['legalities'] = parse_scryfall_legalities(sf_card['legalities'])  # Dict[str, str]
    mtgjson_card['rulings'] = parse_scryfall_rulings(sf_card['rulings_uri'])  # List[Dict[str, str]]
    mtgjson_card['printings'] = parse_scryfall_printings(sf_card['prints_search_uri'])  # List[str]

    card_types: Tuple[List[str], List[str], List[str]] = parse_scryfall_card_types(mtgjson_card['type'])
    mtgjson_card['supertypes'] = card_types[0]  # List[str]
    mtgjson_card['types'] = card_types[1]  # List[str]
    mtgjson_card['subtypes'] = card_types[2]  # List[str]

    # Characteristics that we cannot get from Scryfall
    # Characteristics we have to do further API calls for
    mtgjson_card['foreignData'] = parse_sf_foreign(sf_card['prints_search_uri'], sf_card['set'])  # Dict[str, str]

    if mtgjson_card['multiverseid'] is not None:
        original_soup = download_from_gatherer(mtgjson_card['multiverseid'])
        div_name = layout_options(original_soup)

        mtgjson_card['originalText'] = parse_card_original_text(original_soup, div_name)  # str
        mtgjson_card['originalType'] = parse_card_original_type(original_soup, div_name)  # str

        mtgjson4.LOGGER.info('Parsed {0} from {1}'.format(mtgjson_card.get('name'), sf_card.get('set')))

    mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards


def scryfall_to_mtgjson(sf_cards: List[Dict[str, Any]]) -> List[Any]:
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


def download_from_scryfall(scryfall_url: str) -> Dict[str, Any]:
    """
    Get the data from Scryfall in JSON format using our secret keys
    :param scryfall_url: URL to download JSON data from
    :return: JSON object of the Scryfall data
    """

    header_auth: Dict[str, str] = {}
    auth_status: str = 'with authentication'
    if pathlib.Path(mtgjson4.CONFIG_PATH).is_file():
        # Open and read MTGJSON secret properties
        config = configparser.RawConfigParser()
        config.read(mtgjson4.CONFIG_PATH)
        header_auth = {'Authorization': 'Bearer {}'.format(config.get('Scryfall', 'client_secret'))}
    else:
        auth_status = 'WITHOUT authentication'

    request_api_json: Dict[str, Any] = requests.get(url=scryfall_url, headers=header_auth).json()

    mtgjson4.LOGGER.info('Downloaded ({0}) URL: {1}'.format(auth_status, scryfall_url))

    return request_api_json


def get_scryfall_set(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    mtgjson4.LOGGER.info('Downloading set {} information'.format(set_code))
    set_api_json: Dict[str, Any] = download_from_scryfall(mtgjson4.SCRYFALL_API_SETS + set_code)
    if set_api_json['object'] == 'error':
        mtgjson4.LOGGER.error('Set api download failed for {0}: {1}'.format(set_code, set_api_json))
        return []

    cards_api_url: Optional[str] = set_api_json.get('search_uri')

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = []

    # For each page, append all the data, go to next page
    page_downloaded: int = 1
    while cards_api_url is not None:
        mtgjson4.LOGGER.info('Downloading page {0} of card data for {1}'.format(page_downloaded, set_code))
        page_downloaded += 1

        cards_api_json: Dict[str, Any] = download_from_scryfall(cards_api_url)
        if cards_api_json['object'] == 'error':
            mtgjson4.LOGGER.error('Error downloading {0}: {1}'.format(set_code, cards_api_json))
            return scryfall_cards

        for card in cards_api_json['data']:
            scryfall_cards.append(card)

        if cards_api_json.get('has_more'):
            cards_api_url = cards_api_json.get('next_page')
        else:
            cards_api_url = None

    return scryfall_cards


def download_from_gatherer(card_mid: str) -> bs4.BeautifulSoup:
    """
    Download a specific card from gatherer
    :param card_mid: card id to download
    :return: HTML soup parser of the resulting page
    """
    request_data_html: Any = requests.get(
        url=mtgjson4.GATHERER_CARD,
        params={
            'multiverseid': str(card_mid),
            'printed': 'true',
        },
        headers={},
    )

    soup: bs4.BeautifulSoup = bs4.BeautifulSoup(request_data_html.text, 'html.parser')
    mtgjson4.LOGGER.info('Downloaded URL {}'.format(request_data_html.url))
    return soup


def parse_card_original_type(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    """
    Take the HTML parser and get the printed type
    :param soup: HTML parser object
    :param parse_div: Div to parse (split cards are weird)
    :return: original type
    """
    type_row = soup.find(id=parse_div.format('typeRow'))
    type_row = type_row.findAll('div')[-1]
    type_row = type_row.get_text(strip=True).replace('  ', ' ')
    return str(type_row)


def parse_card_original_text(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    """
    Take the HTML parser and get the printed text
    :param soup: HTML parser object
    :param parse_div: Div to parse (split cards are weird)
    :return: original text
    """
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
    """
    Get the div to parse out (split cards have multiple)
    :param soup: HTML parser object
    :return: div name to parse
    """
    number = soup.find_all('script')
    client_id_tags = ''
    for script in number:
        if 'ClientIDs' in script.get_text():
            client_id_tags = script.get_text()
            break

    # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{} for single cards or
    # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_ctl0*_{} for double cards, * being any int
    div_name = str((client_id_tags.split('ClientIDs.nameRow = \'')[1].split(';')[0])[:-8] + '{}').strip()
    return div_name


def parse_scryfall_rulings(rulings_url: str) -> List[Dict[str, str]]:
    """
    Get the JSON data from Scryfall and convert it to MTGJSON format for rulings
    :param rulings_url: URL to get Scryfall JSON data from
    :return: MTGJSON rulings list
    """
    rules_api_json: Dict[str, Any] = download_from_scryfall(rulings_url)
    if rules_api_json['object'] == 'error':
        mtgjson4.LOGGER.error('Error downloading URL {0}: {1}'.format(rulings_url, rules_api_json))

    sf_rules: List[Dict[str, str]] = []
    mtgjson_rules: List[Dict[str, str]] = []

    for rule in rules_api_json['data']:
        sf_rules.append(rule)

    for sf_rule in sf_rules:
        mtgjson_rule: Dict[str, str] = {'date': sf_rule['published_at'], 'text': sf_rule['comment']}
        mtgjson_rules.append(mtgjson_rule)

    return mtgjson_rules


def parse_scryfall_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Given a card type string, split it up into its raw components: super, sub, and type
    :param card_type: Card type string to parse
    :return: Tuple (super, type, sub) of the card's attributes
    """
    sub_types: List[str] = []
    super_types: List[str] = []
    types: List[str] = []

    if '—' in card_type:
        split_type: List[str] = card_type.split('—')
        supertypes_and_types: str = split_type[0]
        subtypes: str = split_type[1]
        sub_types = [x for x in subtypes.split(' ') if x]
    else:
        supertypes_and_types = card_type

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
    card_legalities: Dict[str, str] = {}
    for key, value in sf_card_legalities.items():
        if value != 'not_legal':
            card_legalities[key] = value.capitalize()

    return card_legalities


def parse_sf_foreign(sf_prints_url: str, set_name: str) -> List[Dict[str, str]]:
    """
    Get the foreign printings information for a specific card
    :param sf_prints_url: URL to get prints from
    :param set_name: Set name
    :return: Foreign entries object
    """
    card_foreign_entries: List[Dict[str, str]] = []

    # Add information to get all languages
    sf_prints_url = sf_prints_url.replace('&unique=prints', '+lang%3Aany&unique=prints')

    prints_api_json: Dict[str, Any] = download_from_scryfall(sf_prints_url)
    if prints_api_json['object'] == 'error':
        mtgjson4.LOGGER.error('No data found for {0}: {1}'.format(sf_prints_url, prints_api_json))
        return []

    for foreign_card in prints_api_json['data']:
        if set_name != foreign_card['set'] or foreign_card['lang'] == 'en':
            continue

        card_foreign_entry: Dict[str, str] = {
            'name': foreign_card.get('printed_name'),
            'text': foreign_card.get('printed_text'),
            'flavor': foreign_card.get('flavor_text'),
            'type': foreign_card.get('printed_type_line')
        }

        try:
            card_foreign_entry['language'] = mtgjson4.LANGUAGE_MAP[foreign_card['lang']]
        except IndexError:
            mtgjson4.LOGGER.warning('Error trying to get language {}'.format(foreign_card))

        try:
            card_foreign_entry['multiverseid'] = foreign_card['multiverse_ids'][0]
        except IndexError:
            mtgjson4.LOGGER.warning('Error trying to get multiverseid {}'.format(foreign_card))

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
    if prints_api_json['object'] == 'error':
        mtgjson4.LOGGER.error('Bad download: {}'.format(sf_prints_url))
        return []

    for card in prints_api_json['data']:
        card_sets.add(card.get('set').upper())

    return list(card_sets)


def remove_unnecessary_fields(card_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove invalid field entries to shrink JSON output size
    """

    def fix_foreign_entries(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # List of dicts
        fd_insert_list = []
        for foreign_info in values:
            fd_insert_dict = {}
            for fd_key, fd_value in foreign_info.items():
                if fd_value is not None:
                    fd_insert_dict[fd_key] = fd_value

            fd_insert_list.append(fd_insert_dict)
        return fd_insert_list

    fixed_dict: List[Dict[str, Any]] = []
    remove_field_if_false: List[str] = ['reserved', 'isOversized', 'isOnlineOnly', 'timeshifted']

    for card_entry in card_list:
        insert_value = {}

        for key, value in card_entry.items():
            if value is not None:
                if key in remove_field_if_false and value is False:
                    continue
                if key == 'foreignData':
                    value = fix_foreign_entries(value)

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


def write_to_file(set_name: str, file_contents: Dict[str, Any], do_cleanup: bool = False) -> None:
    """
    Write the compiled data to a file with the set's code
    Will ensure the output directory exists first
    """
    mtgjson4.COMPILED_OUTPUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(mtgjson4.COMPILED_OUTPUT_DIR, set_name + '.json').open('w', encoding='utf-8') as f:
        if do_cleanup:
            file_contents['cards'] = remove_unnecessary_fields(file_contents['cards'])
            file_contents['tokens'] = remove_unnecessary_fields(file_contents['tokens'])
        json.dump(file_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
        return


def get_all_sets() -> List[str]:
    """
    Grab the set codes (~3 letters) for all sets found
    in the config database.
    :return: List of all set codes found
    """
    downloaded = download_from_scryfall(mtgjson4.SCRYFALL_API_SETS)
    if downloaded['object'] == 'error':
        mtgjson4.LOGGER.error('Downloading Scryfall data failed: {}'.format(downloaded))
        return []

    # Get _ALL_ Scryfall sets
    set_codes: List[str] = [set_obj['code'] for set_obj in downloaded['data']]

    # Remove Scryfall token sets
    set_codes = [s for s in set_codes if not (len(s) >= 4 and s.startswith('t'))]

    return set_codes


def get_compiled_sets() -> List[str]:
    """
    Grab the set codes for all sets that have already been
    compiled and are awaiting use in the set_outputs dir.
    :return: List of all set codes found
    """
    all_paths: List[pathlib.Path] = list(mtgjson4.COMPILED_OUTPUT_DIR.glob('**/*.json'))
    all_sets_found: List[str] = [str(card_set).split('/')[-1][:-5].lower() for card_set in all_paths]
    return all_sets_found


def compile_and_write_outputs() -> None:
    """
    This method class will create the combined output files
    of AllSets.json and AllCards.json
    """
    # Files that should not be combined into compiled outputs
    files_to_ignore: List[str] = ['AllSets.json', 'AllCards.json']

    def create_all_sets() -> Dict[str, Any]:
        """
        This will create the AllSets.json file
        by pulling the compile data from the
        compiled sets and combining them into
        one conglomerate file.
        """
        all_sets_data: Dict[str, Any] = {}

        for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob('*.json'):
            if set_file.name in files_to_ignore:
                continue

            with set_file.open('r', encoding='utf-8') as f:
                file_content = json.load(f)

                # Do not add these to the final output
                file_content.pop('magicRaritiesCode', None)
                file_content.pop('essentialMagicCode', None)
                file_content.pop('useMagicRaritiesNumber', None)
                file_content.pop('code', None)
                file_content.pop('meta', None)
                file_content.pop('mkm_id', None)
                file_content.pop('mkm_name', None)
                file_content.pop('magicCardsInfoCode', None)

                set_name = set_file.name.split('.')[0]
                all_sets_data[set_name] = file_content
        return all_sets_data

    def create_all_cards() -> Dict[str, Any]:
        """
        This will create the AllCards.json file
        by pulling the compile data from the
        compiled sets and combining them into
        one conglomerate file.
        """
        all_cards_data: Dict[str, Any] = {}

        for set_file in mtgjson4.COMPILED_OUTPUT_DIR.glob('*.json'):
            if set_file.name in files_to_ignore:
                continue

            with set_file.open('r', encoding='utf-8') as f:
                file_content = json.load(f)

                for card in file_content['cards']:
                    if card['name'] not in all_cards_data.keys():
                        # Since these can vary from printing to printing, we do not include them in the output
                        card.pop('artist', None)
                        card.pop('cardHash', None)
                        card.pop('flavor', None)
                        card.pop('multiverseid', None)
                        card.pop('number', None)
                        card.pop('originalText', None)
                        card.pop('originalType', None)
                        card.pop('rarity', None)
                        card.pop('variations', None)
                        card.pop('borderColor', None)
                        card.pop('flavorText', None)
                        card.pop('frameVersion', None)
                        card.pop('hasFoil', None)
                        card.pop('hasNonFoil', None)
                        card.pop('isOnlineOnly', None)
                        card.pop('isOversized', None)

                        for foreign in card['foreignData']:
                            foreign.pop('multiverseid', None)

                        all_cards_data[card['name']] = card
        return all_cards_data

    # Actual compilation process of the method
    all_sets = create_all_sets()
    write_to_file('AllSets', all_sets)

    all_cards = create_all_cards()
    write_to_file('AllCards', all_cards)


def main() -> None:
    """
    Main Method
    """
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', metavar='SET', nargs='*', type=str)
    parser.add_argument('-a', '--all-sets', action='store_true')
    parser.add_argument('-c', '--compiled-outputs', action='store_true')
    parser.add_argument('--skip-rebuild', action='store_true')
    parser.add_argument('--skip-cached', action='store_true')
    args = parser.parse_args()

    # Ensure there are args
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)

    if not args.skip_rebuild:
        # Determine sets to build, whether they're passed in as args or all sets in our configs
        set_list: List[str] = get_all_sets() if args.all_sets else args.s
        mtgjson4.LOGGER.info('Sets to compile: {}'.format(set_list))

        # If we had to kill mid-rebuild, we can skip the sets that already were done
        if args.skip_cached:
            sets_compiled_already: List[str] = get_compiled_sets()
            set_list = [s for s in set_list if s not in sets_compiled_already]
            mtgjson4.LOGGER.info('Sets to skip compilation for: {}'.format(sets_compiled_already))
            mtgjson4.LOGGER.info('Sets to compile, after cached sets removed: {}'.format(set_list))

        for set_code in set_list:
            sf_set: List[Dict[str, Any]] = get_scryfall_set(set_code)
            compiled: Dict[str, Any] = build_output_file(sf_set, set_code)
            write_to_file(set_code.upper(), compiled, do_cleanup=True)

    if args.compiled_outputs:
        mtgjson4.LOGGER.info('Compiling AllSets and AllCards')
        compile_and_write_outputs()


if __name__ == '__main__':
    main()
