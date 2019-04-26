"""MTGJSON Version 4 Initializer"""
import contextvars
import logging
import os
import pathlib
import time
from typing import Dict, List

# Maintenance variables
__VERSION__ = "4.4.0"
__VERSION_DATE__ = "2019-04-20"
__MAINTAINER__ = "Zach Halpern (GitHub: @ZeldaZach)"
__MAINTAINER_EMAIL__ = "zahalpern+github@gmail.com"
__REPO_URL__ = "https://github.com/mtgjson/mtgjson"

# Globals -- constant types
SUPERTYPES: List[str] = ["Basic", "Host", "Legendary", "Ongoing", "Snow", "World"]
BASIC_LANDS: List[str] = ["Plains", "Island", "Swamp", "Mountain", "Forest"]

# Globals -- paths
TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
COMPILED_OUTPUT_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("json_" + __VERSION__)
LOG_DIR: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson.properties")
RESOURCE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson4").joinpath("resources")

# Globals -- caches
SESSION_CACHE_EXPIRE_GENERAL: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_TCG: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_SCRYFALL: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_MKM: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_STOCKS: int = 604800  # seconds - 1 week
USE_CACHE: contextvars.ContextVar = contextvars.ContextVar("USE_CACHE")

# Globals -- MKM applications
os.environ["MKM_APP_TOKEN"] = ""
os.environ["MKM_APP_SECRET"] = ""
os.environ["MKM_ACCESS_TOKEN"] = ""
os.environ["MKM_ACCESS_TOKEN_SECRET"] = ""

MTGSTOCKS_BUFFER: str = "20202"
CARD_MARKET_BUFFER: str = "10101"

# Compiled Output Files
ALL_CARDS_OUTPUT: str = "AllCards"
ALL_DECKS_DIR_OUTPUT: str = "AllDeckFiles"
ALL_SETS_DIR_OUTPUT: str = "AllSetFiles"
ALL_SETS_OUTPUT: str = "AllSets"
CARD_TYPES_OUTPUT: str = "CardTypes"
COMPILED_LIST_OUTPUT: str = "CompiledList"
DECK_LISTS_OUTPUT: str = "DeckLists"
KEY_WORDS_OUTPUT: str = "Keywords"
MODERN_OUTPUT: str = "Modern"
REFERRAL_DB_OUTPUT: str = "ReferralMap"
SET_LIST_OUTPUT: str = "SetList"
STANDARD_OUTPUT: str = "Standard"
VERSION_OUTPUT: str = "version"
VINTAGE_OUTPUT: str = "Vintage"

OUTPUT_FILES: List[str] = [
    ALL_CARDS_OUTPUT,
    ALL_DECKS_DIR_OUTPUT,
    ALL_SETS_DIR_OUTPUT,
    ALL_SETS_OUTPUT,
    CARD_TYPES_OUTPUT,
    COMPILED_LIST_OUTPUT,
    DECK_LISTS_OUTPUT,
    KEY_WORDS_OUTPUT,
    MODERN_OUTPUT,
    REFERRAL_DB_OUTPUT,
    SET_LIST_OUTPUT,
    STANDARD_OUTPUT,
    VERSION_OUTPUT,
    VINTAGE_OUTPUT,
]

# Provider tags
SCRYFALL_PROVIDER_ID = "sf"

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
