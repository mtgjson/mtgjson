"""
MTGJSON Version 4 Initializer
"""
import logging
import pathlib
import time
from typing import Dict, List

# Maintenance variables
__VERSION__ = '4.0.0-beta.3'
__VERSION_DATE__ = '2018-09-19'
__MAINTAINER__ = 'Zach Halpern (GitHub: @ZeldaZach)'

# Globals
SCRYFALL_API_SETS: str = 'https://api.scryfall.com/sets/'
GATHERER_CARD: str = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
SUPERTYPES: List[str] = ['Basic', 'Host', 'Legendary', 'Ongoing', 'Snow', 'World']
TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
COMPILED_OUTPUT_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath('set_outputs')
LOG_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath('logs')
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath('mtgjson.properties')

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
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            str(pathlib.Path.joinpath(LOG_DIR, 'mtgjson_' + str(time.strftime('%Y-%m-%d_%H:%M:%S')) + '.log'))),
    ],
)
LOGGER = logging.getLogger()
