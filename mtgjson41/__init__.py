import logging
import pathlib
import time
from typing import List

# Globals
SCRYFALL_API_SETS: str = "https://api.scryfall.com/sets/"
SUPERTYPES: List[str] = ['Basic', 'Legendary', 'Ongoing', 'Snow', 'World']
COMPILED_OUTPUT_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
CONFIG_PATH: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent / 'mtgjson.properties'

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('../logs/mtgjson_' + str(time.strftime('%Y-%m-%d_%H:%M:%S')) + '.log')
    ])
