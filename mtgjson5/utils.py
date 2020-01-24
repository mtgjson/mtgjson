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

    time_now = time.strftime("%Y-%m-%d_%H.%M")

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


def parse_magic_rules_subset(
    magic_rules: str, start_header: str, end_header: str
) -> str:
    """
    Split up the magic rules to get a smaller working subset for parsing
    :param magic_rules: Magic rules to split up
    :param start_header: Start of content
    :param end_header: End of content
    :return: Smaller set of content
    """
    # Keyword actions are found in section XXX
    magic_rules = magic_rules.split(start_header)[2].split(end_header)[0]

    # Windows line endings... yuck
    valid_line_segments = "\n".join(magic_rules.split("\r\n"))

    return valid_line_segments
