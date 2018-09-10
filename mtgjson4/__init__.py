import logging
import pathlib
import time
from typing import List, Dict

# Maintenance variables
__VERSION__ = '4.0.0'
__VERSION_DATE__ = '2018-05-30'

MAINTAINER = 'Zach Halpern (GitHub: @ZeldaZach)'
VERSION_INFO = f'MTGJSON\nVersion {__VERSION__}\n{__VERSION_DATE__}'
DESCRIPTION = 'MTGJSON4 -- Create JSON files for distribution to the public\nMaintained by ' + MAINTAINER

# Globals
SCRYFALL_API_SETS: str = "https://api.scryfall.com/sets/"
SUPERTYPES: List[str] = ['Basic', 'Legendary', 'Ongoing', 'Snow', 'World']
COMPILED_OUTPUT_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
SET_CONFIG_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'set_configs'
CONFIG_PATH: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'mtgjson.properties'
GATHERER_CARD: str = "http://gatherer.wizards.com/Pages/Card/Details.aspx"

LANGUAGE_MAP: Dict[str, str] = {
    'de': 'German',
    'en': 'English',
    'es': 'Spanish',
    'fr': 'French',
    'it': 'Italian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'pt': 'Portuguese (Brazil)',
    'ru': 'Russian',
    'zhs': 'Chinese Simplified',
    'zht': 'Chinese Traditional'
}

SYMBOL_MAP: Dict[str, str] = {
    'White': 'W',
    'Blue': 'U',
    'Black': 'B',
    'Red': 'R',
    'Green': 'G',
    'Colorless': 'C',
    'Variable Colorless': 'X',
    'Snow': 'S',
    'Energy': 'E',
    'Phyrexian White': 'PW',
    'Phyrexian Blue': 'PU',
    'Phyrexian Black': 'PB',
    'Phyrexian Red': 'PR',
    'Phyrexian Green': 'PG',
    'Two or White': '2W',
    'Two or Blue': '2U',
    'Two or Black': '2B',
    'Two or Red': '2R',
    'Two or Green': '2G',
    'White or Blue': 'WU',
    'White or Black': 'WB',
    'Blue or Black': 'UB',
    'Blue or Red': 'UR',
    'Black or Red': 'BR',
    'Black or Green': 'BG',
    'Red or Green': 'RG',
    'Red or White': 'GU',
    'Green or White': 'RW',
    'Green or Blue': 'GW',
    'Half a White': 'HW',
    'Half a Blue': 'HU',
    'Half a Black': 'HB',
    'Half a Red': 'HR',
    'Half a Green': 'HG',
    'Tap': 'T',
    'Untap': 'Q',
    'Infinite': '∞'
}


def get_symbol_short_name(key_to_find: str) -> str:
    """
    Grab the image symbol's alt name
    """
    return SYMBOL_MAP.get(key_to_find, key_to_find)


# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../logs/mtgjson_' + str(time.strftime('%Y-%m-%d_%H:%M:%S')) + '.log')
    ])
