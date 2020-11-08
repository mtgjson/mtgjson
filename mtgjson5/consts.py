"""
MTGJSON Consts for Building
"""
import configparser
import datetime
import hashlib
import os
import pathlib
from typing import Dict, List, Set, Tuple

# Useful MTGJSON Paths - Part 1
TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
RESOURCE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson5").joinpath("resources")
CONFIG_PATH: pathlib.Path = RESOURCE_PATH.joinpath("mtgjson.properties")
ENV_OUT_PATH: pathlib.Path = (
    pathlib.Path(os.environ.get("MTGJSON5_OUTPUT_PATH", TOP_LEVEL_DIR))
    .expanduser()
    .resolve()
)

# Load in MTGJSON config values
CONFIG = configparser.ConfigParser()
if CONFIG_PATH:
    CONFIG.read(str(CONFIG_PATH))

MTGJSON_VERSION: str = CONFIG.get("MTGJSON", "version", fallback="5.X.X")
MTGJSON_BUILD_DATE: str = CONFIG.get(
    "MTGJSON", "date", fallback=""
) or datetime.datetime.today().strftime("%Y-%m-%d")

USE_CACHE: bool = (
    CONFIG.get("MTGJSON", "use_cache", fallback="false").lower().strip() == "true"
)

# Useful MTGJSON Paths - Part 2
LOG_PATH: pathlib.Path = ENV_OUT_PATH.joinpath("mtgjson_logs")
OUTPUT_PATH: pathlib.Path = ENV_OUT_PATH.joinpath(f"mtgjson_build_{MTGJSON_VERSION}")
CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson5_cache")

# Hash Details
HASH_TO_GENERATE = hashlib.sha256()

CARD_MARKET_BUFFER: str = "10101"
CARD_KINGDOM_REFERRAL: str = (
    "?partner=mtgjson&utm_source=mtgjson&utm_medium=affiliate&utm_campaign=mtgjson"
)
# TCGPLAYER_REFERRAL: str = "?partner=mtgjson&utm_campaign=affiliate&utm_medium=mtgjson&utm_source=mtgjson"
# CARD_MARKET_REFERRAL: str = "?utm_campaign=card_prices&utm_medium=text&utm_source=mtgjson"

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
    "ph": "Phyrexian",
    "px": "Phyrexian",
}
WIZARDS_SUPPORTED_LANGUAGES: List[Tuple[str, str]] = [
    ("zh-hans", "Chinese Simplified"),
    ("zh-hant", "Chinese Traditional"),
    ("fr", "French"),
    ("de", "German"),
    ("it", "Italian"),
    ("ja", "Japanese"),
    ("ko", "Korean"),
    ("pt-br", "Portuguese (Brazil)"),
    ("ru", "Russian"),
    ("es", "Spanish"),
    ("en", "English"),
]
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
