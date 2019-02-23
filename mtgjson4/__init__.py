"""MTGJSON Version 4 Initializer"""
import logging
import pathlib
import time
from typing import Dict, List

# Maintenance variables
__VERSION__ = "4.3.0"
__VERSION_DATE__ = "2019-02-22"
__MAINTAINER__ = "Zach Halpern (GitHub: @ZeldaZach)"
__MAINTAINER_EMAIL__ = "zahalpern+github@gmail.com"
__REPO_URL__ = "https://github.com/mtgjson/mtgjson4"

# Globals
SUPERTYPES: List[str] = ["Basic", "Host", "Legendary", "Ongoing", "Snow", "World"]
BASIC_LANDS: List[str] = ["Plains", "Island", "Swamp", "Mountain", "Forest"]
TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
COMPILED_OUTPUT_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("set_outputs")
LOG_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson.properties")
RESOURCE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson4").joinpath("resources")

# Compiled Output Files
ALL_SETS_OUTPUT: str = "AllSets"
ALL_CARDS_OUTPUT: str = "AllCards"
SET_LIST_OUTPUT: str = "SetList"
COMPILED_LIST_OUTPUT: str = "CompiledList"
KEY_WORDS_OUTPUT: str = "Keywords"
VERSION_OUTPUT: str = "version"
STANDARD_OUTPUT: str = "Standard"
MODERN_OUTPUT: str = "Modern"
VINTAGE_OUTPUT: str = "Vintage"
REFERRAL_DB_OUTPUT: str = "ReferralMap"
ALL_SETS_DIR_OUTPUT: str = "AllSetFiles"
ALL_DECKS_DIR_OUTPUT: str = "AllDeckFiles"
CARD_TYPES_OUTPUT: str = "CardTypes"
DECK_LISTS_OUTPUT: str = "DeckLists"

LANGUAGE_MAP: Dict[str, str] = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese (Brazil)",
    "ja": "Japanese",
    "ko": "Korean",
    "ru": "Russian",
    "zhs": "Chinese Simplified",
    "zht": "Chinese Traditional",
    "he": "Hebrew",
    "la": "Latin",
    "grc": "Ancient Greek",
    "ar": "Arabic",
    "sa": "Sanskrit",
    "px": "Phyrexian",
}

# File names that can't exist on Windows
BANNED_FILE_NAMES: List[str] = [
    "AUX",
    "COM0",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "CON",
    "LPT0",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
    "NUL",
    "PRN",
]


def init_logger() -> None:
    """
    Logging configuration
    """
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                str(
                    LOG_DIR.joinpath(
                        "mtgjson_" + str(time.strftime("%Y-%m-%d_%H.%M.%S")) + ".log"
                    )
                )
            ),
        ],
    )
