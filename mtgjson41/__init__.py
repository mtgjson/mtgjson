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
SET_CONFIG_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent / 'set_configs'
CONFIG_PATH: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'mtgjson.properties'

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

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../logs/mtgjson_' + str(time.strftime('%Y-%m-%d_%H:%M:%S')) + '.log')
    ])
