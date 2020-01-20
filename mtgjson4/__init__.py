"""MTGJSON Version 4 Initializer"""
import contextvars
import logging
import os
import pathlib
import time
from typing import Dict, List, Set

# Maintenance variables
__VERSION__ = "4.6.2"
__VERSION_DATE__ = "2019-12-01"
__PRICE_UPDATE_DATE__ = "2019-12-01"
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
PROJECT_CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson_cache")

# Globals -- caches
SESSION_CACHE_EXPIRE_GENERAL: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_TCG: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_SCRYFALL: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_MKM: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_STOCKS: int = 604800  # seconds - 1 week
SESSION_CACHE_EXPIRE_CH: int = 43200  # seconds - 1 day
USE_CACHE: contextvars.ContextVar = contextvars.ContextVar("USE_CACHE")
PRETTY_OUTPUT: contextvars.ContextVar = contextvars.ContextVar("PRETTY_OUTPUT")

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
ALL_SETS_OUTPUT: str = "AllPrintings"
CARD_TYPES_OUTPUT: str = "CardTypes"
COMPILED_LIST_OUTPUT: str = "CompiledList"
DECK_LISTS_OUTPUT: str = "DeckLists"
KEY_WORDS_OUTPUT: str = "Keywords"
PRICES_OUTPUT: str = "AllPrices"
REFERRAL_DB_OUTPUT: str = "ReferralMap"
SET_LIST_OUTPUT: str = "SetList"
VERSION_OUTPUT: str = "version"

STANDARD_OUTPUT: str = "StandardPrintings"
PIONEER_OUTPUT: str = "PioneerPrintings"
MODERN_OUTPUT: str = "ModernPrintings"
VINTAGE_OUTPUT: str = "VintagePrintings"
LEGACY_OUTPUT: str = "LegacyPrintings"

STANDARD_CARDS_OUTPUT: str = "StandardCards"
PIONEER_CARDS_OUTPUT: str = "PioneerCards"
MODERN_CARDS_OUTPUT: str = "ModernCards"
VINTAGE_CARDS_OUTPUT: str = "VintageCards"
LEGACY_CARDS_OUTPUT: str = "LegacyCards"
PAUPER_CARDS_OUTPUT: str = "PauperCards"

SUPPORTED_FORMAT_OUTPUTS: Set[str] = {
    "standard",
    "pioneer",
    "modern",
    "legacy",
    "vintage",
    "pauper",
}

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
    VERSION_OUTPUT,
    STANDARD_OUTPUT,
    PIONEER_OUTPUT,
    MODERN_OUTPUT,
    VINTAGE_OUTPUT,
    LEGACY_OUTPUT,
    STANDARD_CARDS_OUTPUT,
    PIONEER_CARDS_OUTPUT,
    MODERN_CARDS_OUTPUT,
    VINTAGE_CARDS_OUTPUT,
    LEGACY_CARDS_OUTPUT,
    PAUPER_CARDS_OUTPUT,
    PRICES_OUTPUT,
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

# Globals -- non-english sets
NON_ENGLISH_SETS: List[str] = [
    "PMPS11",
    "PS11",
    "PMPS10",
    "PMPS09",
    "PMPS08",
    "PMPS07",
    "PMPS06",
    "PSA1",
    "PMPS",
    "PJJT",
    "PHJ",
    "PRED",
    "REN",
    "RIN",
    "4BB",
    "FBB",
]


def init_logger() -> None:
    """
    Logging configuration
    """
    PROJECT_CACHE_PATH.mkdir(exist_ok=True)
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
