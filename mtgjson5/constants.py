"""
MTGJSON Constants that cannot be changed and are hardcoded intentionally
"""

import datetime
import hashlib
import os
import pathlib
from typing import Dict, Set

TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
RESOURCE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson5").joinpath("resources")
CONFIG_PATH: pathlib.Path = RESOURCE_PATH.joinpath("mtgjson.properties")
ENV_OUT_PATH: pathlib.Path = (
    pathlib.Path(os.environ.get("MTGJSON5_OUTPUT_PATH", TOP_LEVEL_DIR))
    .expanduser()
    .resolve()
)
OUTPUT_PATH: pathlib.Path = ENV_OUT_PATH.joinpath("output")

LOG_PATH: pathlib.Path = ENV_OUT_PATH.joinpath("mtgjson_logs")

MTGJSON_BUILD_DATE: str = datetime.datetime.today().strftime("%Y-%m-%d")

CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson5_cache")

HASH_TO_GENERATE = hashlib.sha256()

CARD_MARKET_BUFFER: str = "10101"
CARD_KINGDOM_REFERRAL: str = (
    "?partner=mtgjson&utm_source=mtgjson&utm_medium=affiliate&utm_campaign=mtgjson"
)

FOREIGN_SETS: Set[str] = {
    "PMPS11",
    "PS11",
    "PSAL",
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
TOKEN_LAYOUTS = {"token", "double_faced_token", "emblem", "art_series"}
SUPER_TYPES: Set[str] = {"Basic", "Host", "Legendary", "Ongoing", "Snow", "World"}
BASIC_LAND_NAMES: Set[str] = {"Plains", "Island", "Swamp", "Mountain", "Forest"}
LANGUAGE_MAP: Dict[str, str] = {
    "grc": "Ancient Greek",
    "ar": "Arabic",
    "zhs": "Chinese Simplified",
    "zht": "Chinese Traditional",
    "en": "English",
    "fr": "French",
    "de": "German",
    "he": "Hebrew",
    "it": "Italian",
    "ja": "Japanese",
    "ko": "Korean",
    "la": "Latin",
    "ph": "Phyrexian",
    "px": "Phyrexian",
    "pt": "Portuguese (Brazil)",
    "qya": "Quenya",
    "ru": "Russian",
    "sa": "Sanskrit",
    "es": "Spanish",
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
SUPPORTED_FORMAT_OUTPUTS: Set[str] = {
    "standard",
    "pioneer",
    "modern",
    "legacy",
    "vintage",
    "pauper",
}
SUPPORTED_SET_TYPES: Set[str] = {
    "expansion",
    "core",
    "draft_innovation",
    "commander",
    "masters",
}
MULTI_WORD_SUB_TYPES: Set[str] = {"Time Lord"}
