"""
Construct Prices for MTGJSON
"""
import configparser
import datetime
import lzma
import pathlib
import shutil
from typing import Any, Dict

import dateutil.relativedelta
import git
import simplejson as json

from .consts import CACHE_PATH, OUTPUT_PATH
from .providers import CardhoarderProvider, TCGPlayerProvider
from .utils import get_thread_logger

LOGGER = get_thread_logger()


def download_prices_archive(
    gist_repo_name: str, file_name: str, github_repo_local_path: pathlib.Path
) -> Any:
    """
    Grab the contents from a gist file
    :param gist_repo_name: Gist repo name
    :param file_name: File to open from Gist
    :param github_repo_local_path: Where to checkout the repo to
    :return: File content
    """
    LOGGER.info("Cloning GitHub Repo...")
    github_url = f"https://gist.github.com/{gist_repo_name}"

    if github_repo_local_path.is_dir():
        LOGGER.warning("Deleting old copy of GitHub repo first...")
        shutil.rmtree(github_repo_local_path)

    git_sh = git.cmd.Git()
    git_sh.clone(github_url, github_repo_local_path, depth=1)

    # with lzma.open(github_repo_local_path.joinpath(file_name)) as file:
    #     return json.load(file)
    with github_repo_local_path.joinpath(file_name).open() as file:
        return json.load(file)


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
        LOGGER.warning(f"No changes found to GitHub repo, skipping...")

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


def build_prices() -> None:
    """
    The full build prices operation
    Prune & Update remote database
    """
    # Get today's price database
    LOGGER.info("Building new price data")
    today_prices = build_today_prices()

    # Config values for GitHub
    config = TCGPlayerProvider().get_configs()
    github_repo_name = config.get("GitHub", "repo_name")
    github_file_name = config.get("GitHub", "file_name")
    github_local_path = CACHE_PATH.joinpath("GitHub-PricesArchive")

    # Get the current working database
    LOGGER.info("Downloading old price data")
    archive_prices = download_prices_archive(
        github_repo_name, github_file_name, github_local_path
    )

    # Update local copy of database
    LOGGER.info("Merging price data")
    archive_prices = deep_merge_dictionaries(archive_prices, today_prices)

    # Prune local copy of database
    LOGGER.info("Pruning price data")
    prune_prices_archive(archive_prices)

    # Push changes to remote database
    LOGGER.info("Uploading price data")
    upload_prices_archive(config, github_local_path, archive_prices)
