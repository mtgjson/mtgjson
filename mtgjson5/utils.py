"""
MTGJSON simple utilities
"""

import collections
import hashlib
import json
import logging
import os
import pathlib
import time
from collections.abc import Iterator
from typing import Any

import polars as pl
import requests

from . import constants
from .mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)


def init_logger() -> None:
    """
    Initialize the main system logger
    """
    constants.LOG_PATH.mkdir(parents=True, exist_ok=True)

    start_time = time.strftime("%Y-%m-%d_%H.%M.%S")

    logging.basicConfig(
        level=(
            logging.DEBUG
            if os.environ.get("MTGJSON5_DEBUG", "").lower() in ["true", "1"]
            else logging.INFO
        ),
        format="[%(levelname)s] %(asctime)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                str(constants.LOG_PATH.joinpath(f"mtgjson_{start_time}.log"))
            ),
        ],
        force=True,
    )
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def url_keygen(unique_seed: int | str, with_leading: bool = True) -> str:
    """
    Generates a key that MTGJSON will use for redirection
    :param unique_seed: Link seed
    :param with_leading: Should URL be included
    :return: URL Key
    """
    return_value = "https://mtgjson.com/links/" if with_leading else ""
    return f"{return_value}{hashlib.sha256(str(unique_seed).encode()).hexdigest()[:16]}"


def to_camel_case(snake_str: str) -> str:
    """
    Convert "snake_case" => "camelCase"
    :param snake_str: Snake String
    :return: Camel String
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def to_snake_case(camel_str: str) -> str:
    """
    Convert "camelCase" => "snake_case"
    :param camel_str: Camel String
    :return: Snake String
    """
    return "".join(
        ["_" + char.lower() if char.isupper() else char for char in camel_str]
    ).lstrip("_")


def parse_magic_rules_subset(
    magic_rules: str, start_header: str = "", end_header: str = ""
) -> str:
    """
    Split up the magic rules to get a smaller working subset for parsing
    :param magic_rules: Magic rules to split up
    :param start_header: Start of content
    :param end_header: End of content
    :return: Smaller set of content
    """
    # Keyword actions are found in section XXX
    if start_header and end_header:
        magic_rules = magic_rules.split(start_header)[2].split(end_header)[0]

    # Windows line endings... yuck
    valid_line_segments = "\n".join(magic_rules.splitlines())

    return valid_line_segments


def sort_internal_lists(data: Any) -> Any:
    """
    Sort all lists & sets within a given data structure
    :param data: Data structure to internally sort
    :return Data structure with sorted lists
    """
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = sort_internal_lists(value)
    elif isinstance(data, (set, list)):
        return sorted([x for x in list(data) if x is not None])

    return data


def get_file_hash(file_to_hash: pathlib.Path, block_size: int = 65536) -> str:
    """
    Given a file, generate a hash of the contents
    :param file_to_hash: File to generate the hash of
    :param block_size: How big a chunk to read in at a time
    :return file hash
    """
    if not file_to_hash.is_file():
        LOGGER.warning(f"Unable to find {file_to_hash}, no hashes generated")
        return ""

    # Hash can be adjusted in consts.py file
    hash_operation = constants.HASH_TO_GENERATE.copy()

    with file_to_hash.open("rb") as file:
        while True:
            data = file.read(block_size)
            if not data:
                break
            hash_operation.update(data)

    return hash_operation.hexdigest()


def get_str_or_none(value: Any) -> str | None:
    """
    Given a value, get its string representation
    or None object
    :param value: Input value
    :return String value of input or None
    """
    if not value:
        return None

    return str(value)


def send_push_notification(message: str) -> bool:
    """
    Send a push notification to project maintainers.
    These alerts can be disabled by removing the Pushover
    category from the properties file.
    :param message: Message to send
    :return If the message send successfully to everyone
    """
    if not MtgjsonConfig().has_section("Pushover"):
        LOGGER.warning("Pushover section not established. Skipping alerts.")
        return False

    pushover_app_token = MtgjsonConfig().get("Pushover", "app_token")
    pushover_app_users = list(
        filter(None, MtgjsonConfig().get("Pushover", "user_tokens").split(","))
    )

    if not (pushover_app_token and pushover_app_token):
        LOGGER.warning("Pushover keys values missing. Skipping alerts")
        return False

    all_succeeded = True
    for user in pushover_app_users:
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": pushover_app_token,
                "user": user,
                "title": f"MTGJSON {MtgjsonConfig().mtgjson_version}",
                "message": message,
            },
            timeout=60,
        )
        if not response.ok:
            LOGGER.warning(f"Error sending Pushover notification: {response.text}")
            all_succeeded = False

    return all_succeeded


def get_all_entities_from_content(
    all_printings_content: dict[str, Any], include_sealed_product: bool = False
) -> list[dict[str, Any]]:
    """
    Convert the content of AllPrintings into a list of entity objects (mostly cards, can include sealedProduct)
    :param all_printings_content: Content of AllPrintings
    :param include_sealed_product: Should sealedProduct be included in results
    :return List of cards
    """
    entities_with_set_code = []
    for value in all_printings_content.values():
        for entity in (
            value.get("cards", [])
            + value.get("tokens", [])
            + (value.get("sealedProduct", []) if include_sealed_product else [])
        ):
            entities_with_set_code.append(entity)
    return entities_with_set_code


def get_all_entities(
    all_printings_path: pathlib.Path, include_sealed_product: bool = False
) -> Iterator[dict[str, Any]]:
    """
    Grab every card, token, and possible sealedProduct object from an AllPrintings file for future iteration
    :param all_printings_path: AllPrintings.json to refer when building
    :param include_sealed_product: Should sealedProduct be included in results
    :return Iterator for all card and token objects
    """
    all_printings_path = all_printings_path.expanduser()
    if not all_printings_path.exists():
        LOGGER.error(f"File {all_printings_path} does not exist, cannot iterate")
        return

    with all_printings_path.open(encoding="utf-8") as f:
        file_contents = json.load(f).get("data", {})

    yield from get_all_entities_from_content(file_contents, include_sealed_product)


def generate_entity_mapping(
    all_printings_path: pathlib.Path,
    left_side_components: tuple[str, ...],
    right_side_components: tuple[str, ...],
    include_sealed_product: bool = False,
) -> dict[str, set[Any]]:
    """
    Construct a mapping from one component of the card to another.
    The components are nested ops to get to the final value.
    :param all_printings_path: AllPrintings file to load card data from
    :param left_side_components: Inner left hand side components ([foo, bar] => card[foo][bar])
    :param right_side_components: Inner right hand side components ([foo, bar] => card[foo][bar])
    :param include_sealed_product: Should sealedProduct be included in entities
    :return Dict mapping from left components => right components
    """
    dump_map: dict[str, set[Any]] = collections.defaultdict(set)

    for entity in get_all_entities(all_printings_path, include_sealed_product):
        try:
            key = entity
            for inside_component in left_side_components:
                key = key[inside_component]

            value = entity
            for inside_component in right_side_components:
                value = value[inside_component]

            if isinstance(value, (set, list)):
                for entry in value:
                    dump_map[str(key)].add(entry)
            else:
                dump_map[str(key)].add(value)
        except KeyError:
            pass

    return dump_map


def load_local_set_data() -> dict[str, dict[str, Any]]:
    """
    Loads the local set data
    """
    with constants.RESOURCE_PATH.joinpath("additional_sets.json").open(
        encoding="utf-8"
    ) as f:
        data: dict[str, dict[str, Any]] = json.load(f)
    return data


def recursive_sort(unsorted_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively sort a dictionary's inner keys and values, as necessary
    """
    return {
        key: recursive_sort(value) if isinstance(value, dict) else value
        for key, value in sorted(unsorted_dict.items())
    }


def deep_sort_keys(obj: Any) -> Any:
    """
    Recursively sort dictionary keys, including within lists.
    """
    if isinstance(obj, dict):
        return {key: deep_sort_keys(value) for key, value in sorted(obj.items())}
    if isinstance(obj, list):
        return [deep_sort_keys(item) for item in obj]
    return obj


def get_expanded_set_codes(
    set_codes: str | list[str] | None = None,
) -> list[str]:
    """
    Expand set codes to include all sets with matching release dates
    in order to selectively build Decks and other potentially cross-set products
    Returns:
        List of uppercase set codes (original + expanded by release date).
    """
    response = requests.get("https://api.scryfall.com/sets", timeout=30)
    response.raise_for_status()
    data = response.json()

    # filter out unwanted set types
    sets_lf = (
        pl.DataFrame(data["data"])
        .filter(
            ~pl.col("set_type").is_in(["memorabilia", "promo", "alchemy"])
            & ~pl.col("name").str.contains("Art Series")
        )
        .lazy()
    )

    # return all sets if None
    if set_codes is None:
        return sets_lf.select("code").collect().to_series().to_list()

    # Normalize to list
    codes_list: list[str] = [set_codes] if isinstance(set_codes, str) else set_codes

    # Get release dates for requested sets
    release_dates = (
        sets_lf.filter(pl.col("code").is_in(codes_list))
        .select("released_at")
        .unique()
        .collect()
        .to_series()
    )

    # Find all sets with matching release dates, excluding unwanted types
    return (
        sets_lf.filter(
            pl.col("released_at").is_in(release_dates)
            & (
                pl.col("code").is_in(codes_list)
                | pl.col("parent_set_code").is_in(codes_list)
            )
        )
        .select("code")
        .collect()
        .to_series()
        .to_list()
    )
