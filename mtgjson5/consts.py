"""
Const values of MTGJSON
"""
import pathlib
from typing import Dict, Set

MTGJSON_VERSION: str = "5.0.0"

# Useful paths within the MTGJSON system
TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson.properties")
CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson5_cache")
LOG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")
OUTPUT_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(f"json_{MTGJSON_VERSION}")
RESOURCE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson5").joinpath("resources")

SILVER_SETS_TO_NOT_UNIQUIFY: Set[str] = {"HHO", "UNH"}

CARD_MARKET_BUFFER: str = "10101"

FOREIGN_SETS: Set[str] = {
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
}
SUPER_TYPES: Set[str] = {"Basic", "Host", "Legendary", "Ongoing", "Snow", "World"}
BASIC_LAND_NAMES: Set[str] = {"Plains", "Island", "Swamp", "Mountain", "Forest"}
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
SYMBOL_MAP: Dict[str, str] = {
    "White": "W",
    "Blue": "U",
    "Black": "B",
    "Red": "R",
    "Green": "G",
    "Colorless": "C",
    "Variable Colorless": "X",
    "Snow": "S",
    "Energy": "E",
    "Phyrexian White": "PW",
    "Phyrexian Blue": "PU",
    "Phyrexian Black": "PB",
    "Phyrexian Red": "PR",
    "Phyrexian Green": "PG",
    "Two or White": "2W",
    "Two or Blue": "2U",
    "Two or Black": "2B",
    "Two or Red": "2R",
    "Two or Green": "2G",
    "White or Blue": "WU",
    "White or Black": "WB",
    "Blue or Black": "UB",
    "Blue or Red": "UR",
    "Black or Red": "BR",
    "Black or Green": "BG",
    "Red or Green": "RG",
    "Red or White": "GU",
    "Green or White": "RW",
    "Green or Blue": "GW",
    "Half a White": "HW",
    "Half a Blue": "HU",
    "Half a Black": "HB",
    "Half a Red": "HR",
    "Half a Green": "HG",
    "Tap": "T",
    "Untap": "Q",
    "Infinite": "∞",
}
BAD_FILE_NAMES: Set[str] = {
    # File names that can't exist on Windows
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
}

USE_CACHE: bool = True


def __update_cache_status(new_cache_status: bool) -> None:
    """
    Update the consts Cache status
    :param new_cache_status: New cache status
    """
    global USE_CACHE
    USE_CACHE = new_cache_status
