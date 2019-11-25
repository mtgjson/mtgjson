"""
Const values of MTGJSON
"""
import logging
import pathlib
import time
from typing import List

MTGJSON_VERSION = "5.0.0"

TOP_LEVEL_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("mtgjson.properties")
CACHE_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath(".mtgjson5_cache")
LOG_PATH: pathlib.Path = TOP_LEVEL_DIR.joinpath("logs")

FOREIGN_SETS: List[str] = []
SUPER_TYPES: List[str] = ["Basic", "Host", "Legendary", "Ongoing", "Snow", "World"]


def init_logger() -> None:
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
