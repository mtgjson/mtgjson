"""
Construct Prices for MTGJSON
"""
import configparser
import datetime
import logging
import lzma
import pathlib
import shutil
from typing import Any, Dict

import dateutil.relativedelta
import git
import requests
import simplejson as json

from .classes import MtgjsonPricesObject, MtgjsonSetObject
from .consts import CACHE_PATH, OUTPUT_PATH
from .providers import CardhoarderProvider, TCGPlayerProvider

LOGGER = logging.getLogger(__name__)


def download_prices_archive(
    gist_repo_name: str, file_name: str, github_repo_local_path: pathlib.Path
) -> Dict[str, Dict[str, float]]:
    """
    Grab the contents from a gist file
    :param gist_repo_name: Gist repo name
    :param file_name: File to open from Gist
    :param github_repo_local_path: Where to checkout the repo to
    :return: File content
    """
    github_url = f"https://gist.github.com/{gist_repo_name}"

    if github_repo_local_path.is_dir():
        LOGGER.warning("Deleting Old Price Data Repo")
        shutil.rmtree(github_repo_local_path)

    LOGGER.info("Cloning Price Data Repo")
    git_sh = git.cmd.Git()
    git_sh.clone(github_url, github_repo_local_path, depth=1)

    with lzma.open(github_repo_local_path.joinpath(file_name)) as file:
        return dict(json.load(file))


def upload_prices_archive(
    config: configparser.RawConfigParser,
    github_repo_local_path: pathlib.Path,
    content: Any,
) -> None:
    """
    Upload prices archive back to GitHub
    :param config Config for GitHub
    :param github_repo_local_path: Local file system file
    :param content: File content
    """
    github_username = config.get("GitHub", "username")
    github_api_token = config.get("GitHub", "api_key")
    file_name = config.get("GitHub", "file_name")
    github_repo_name = config.get("GitHub", "repo_name")

    # Compress the file to upload for speed and storage savings
    with lzma.open(github_repo_local_path.joinpath(file_name), "w") as file:
        file.write(json.dumps(content).encode("utf-8"))

    try:
        repo = git.Repo(github_repo_local_path)

        # Update remote to allow pushing
        repo.git.remote(
            "set-url",
            "origin",
            f"https://{github_username}:{github_api_token}@gist.github.com/{github_repo_name}.git",
        )

        repo.git.commit("-am", "auto-push")
        origin = repo.remote()
        origin.push()
        LOGGER.info("Pushed changes to GitHub repo")
    except git.GitCommandError:
        LOGGER.warning(f"No changes found to GitHub repo, skipping")

    shutil.rmtree(github_repo_local_path)


def prune_prices_archive(content: Dict[str, Any], months: int = 3) -> None:
    """
    Prune entries from the CardHoarder database in which entries are
    older than X months
    :param content: Database to modify
    :param months: How many months back should we keep (default = 3)
    """
    prune_date = datetime.date.today() + dateutil.relativedelta.relativedelta(
        months=-months
    )

    for format_dicts in content.values():
        for date_price in format_dicts.values():
            # Skip UUID and any other meta data we may add in future
            if not isinstance(date_price, dict):
                continue

            keys_to_prune = [
                key_date
                for key_date in date_price.keys()
                if datetime.datetime.strptime(key_date, "%Y-%m-%d").date() < prune_date
            ]

            for key in keys_to_prune:
                del date_price[key]


def deep_merge_dictionaries(
    dictionary_one: Dict[str, Any], dictionary_two: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Merge two dictionaries together, recursively
    :param dictionary_one: Dict 1
    :param dictionary_two: Dict 2
    :return: Combined Dictionaries
    """
    new_dictionary = dictionary_one.copy()

    new_dictionary.update(
        {
            key: deep_merge_dictionaries(new_dictionary[key], dictionary_two[key])
            if isinstance(new_dictionary.get(key), dict)
            and isinstance(dictionary_two[key], dict)
            else dictionary_two[key]
            for key in dictionary_two.keys()
        }
    )

    return new_dictionary


def build_today_prices() -> Dict[str, Any]:
    """
    Get today's prices from upstream sources and combine them together
    :return: Today's prices (to be merged into archive)
    """
    if not OUTPUT_PATH.joinpath("AllPrintings.json").is_file():
        LOGGER.error(f"Unable to build prices. AllPrintings not found in {OUTPUT_PATH}")
        return {}

    cardhoarder_prices = CardhoarderProvider().generate_today_price_dict()
    tcgplayer_prices = TCGPlayerProvider().generate_today_price_dict(
        OUTPUT_PATH.joinpath("AllPrintings.json")
    )

    cardhoarder_prices_json = json.loads(json.dumps(cardhoarder_prices, for_json=True))
    tcgplayer_prices_json = json.loads(json.dumps(tcgplayer_prices, for_json=True))

    final_results = deep_merge_dictionaries(
        cardhoarder_prices_json, tcgplayer_prices_json
    )

    return final_results


def get_price_archive_data() -> Dict[str, Dict[str, float]]:
    """
    Download compiled MTGJSON price data
    :return: MTGJSON price data
    """
    config = TCGPlayerProvider().get_configs()

    if not (
        config.get("GitHub", "repo_name")
        and config.get("GitHub", "repo_name")
        and config.get("GitHub", "repo_name")
    ):
        LOGGER.warning("GitHub keys not established. Skipping price archives")
        return {}

    # Config values for GitHub
    github_repo_name = config.get("GitHub", "repo_name")
    github_file_name = config.get("GitHub", "file_name")
    github_local_path = CACHE_PATH.joinpath("GitHub-PricesArchive")

    # Get the current working database
    LOGGER.info("Downloading Price Data Repo")
    return download_prices_archive(
        github_repo_name, github_file_name, github_local_path
    )


def download_old_all_printings() -> None:
    """
    Download the hosted version of AllPrintings from MTGJSON
    for future consumption
    """
    file_bytes = b""
    file_data = requests.get(
        f"https://mtgjson.com/files/AllPrintings.json.xz", stream=True
    )
    for chunk in file_data.iter_content(chunk_size=1024 * 36):
        if chunk:
            file_bytes += chunk

    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.joinpath("AllPrintings.json").open("w", encoding="utf8") as f:
        f.write(lzma.decompress(file_bytes).decode())


def build_prices() -> Dict[str, Any]:
    """
    The full build prices operation
    Prune & Update remote database
    :return Latest prices
    """
    # We'll need AllPrintings.json to handle this
    if not OUTPUT_PATH.joinpath("AllPrintings.json").is_file():
        LOGGER.info("AllPrintings not found, attempting to download")
        download_old_all_printings()

    # Get today's price database
    LOGGER.info("Building new price data")
    today_prices = build_today_prices()

    if not today_prices:
        LOGGER.warning(
            "TCGPlayer and CardHoarder keys not established. No prices generated"
        )
        return {}

    archive_prices = get_price_archive_data()

    # Update local copy of database
    LOGGER.info("Merging price data")
    archive_prices = deep_merge_dictionaries(archive_prices, today_prices)

    # Prune local copy of database
    LOGGER.info("Pruning price data")
    prune_prices_archive(archive_prices)

    # Push changes to remote database
    LOGGER.info("Uploading price data")
    config = TCGPlayerProvider().get_configs()
    github_local_path = CACHE_PATH.joinpath("GitHub-PricesArchive")
    upload_prices_archive(config, github_local_path, archive_prices)

    # Return the latest prices
    CACHE_PATH.joinpath("last_price_build_time").touch()
    return archive_prices


def add_prices_to_mtgjson_set(
    mtgjson_set: MtgjsonSetObject, price_data_cache: Dict[str, Any]
) -> None:
    """
    Add the final pieces to the set (i.e. Price data)
    :param mtgjson_set: Part 1 build
    :param price_data_cache: Data cache to pull entries from
    """
    for mtgjson_card_object in mtgjson_set.cards:
        single_price_entries: Dict[str, Dict[str, float]] = {}
        data_entry = price_data_cache.get(mtgjson_card_object.uuid, {})
        for key, value in data_entry.items():
            if not isinstance(value, dict):
                continue

            if value:
                max_value = max(value)
                single_price_entries[key] = {max_value: value[max_value]}

        mtgjson_card_object.prices = MtgjsonPricesObject(
            mtgjson_card_object.uuid, single_price_entries
        )
