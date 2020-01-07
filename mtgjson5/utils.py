"""
MTGJSON simple utilities
"""
import hashlib
import inspect
import logging
import time
from typing import Union

from .consts import LOG_PATH


def url_keygen(prod_id: Union[int, str], with_leading: bool = True) -> str:
    """
    Generates a key that MTGJSON will use for redirection
    :param prod_id: Seed
    :param with_leading: Should URL be included
    :return: URL Key
    """
    return_value = "https://mtgjson.com/links/" if with_leading else ""
    return f"{return_value}{hashlib.sha256(str(prod_id).encode()).hexdigest()[:16]}"


def to_camel_case(snake_str: str) -> str:
    """
    Convert "snake_case" => "snakeCase"
    :param snake_str: Snake String
    :return: Camel String
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def get_thread_logger() -> logging.Logger:
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

    frame = inspect.stack()[1]
    file_name = frame[0].f_code.co_filename
    return logging.getLogger(file_name)
