"""
Const values of MTGJSON
"""
import logging
import pathlib
import time
from typing import Dict, List, Set

MTGJSON_VERSION = "5.0.0"

TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson.properties")
CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson5_cache")
LOG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")
OUTPUT_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(f"json_{MTGJSON_VERSION}")

FOREIGN_SETS: List[str] = []
SUPER_TYPES: List[str] = ["Basic", "Host", "Legendary", "Ongoing", "Snow", "World"]
BASIC_LAND_NAMES: List[str] = ["Plains", "Island", "Swamp", "Mountain", "Forest"]

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

SYMBOL_MAP = {
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


# File names that can't exist on Windows
BAD_FILE_NAMES: Set[str] = {
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


def init_thread_logger() -> None:
    """
    Logging configuration
    """
    LOG_PATH.mkdir(exist_ok=True)

    time_now = time.strftime("%Y-%m-%d_%H.%M.%S")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(LOG_PATH.joinpath(f"mtgjson_{time_now}.log"))),
        ],
    )


def to_camel_case(snake_str: str) -> str:
    """
    Convert "snake_case" => "snakeCase"
    :param snake_str: Snake String
    :return: Camel String
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])
