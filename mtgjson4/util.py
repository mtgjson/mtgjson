"""Utility functions."""
import contextvars
import datetime
import hashlib
import json
import logging
import lzma
import pathlib
import re
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Set
import unicodedata

import git
import requests
import requests.adapters
import requests_cache
import urllib3.util.retry

import mtgjson4

LOGGER = logging.getLogger(__name__)
SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")
STANDARD_SETS: contextvars.ContextVar = contextvars.ContextVar("STANDARD_SETS")

temp_working_dir: pathlib.Path = pathlib.Path(str(tempfile.mkdtemp(prefix="mtgjson_")))

STANDARD_API_URL: str = "https://whatsinstandard.com/api/v5/sets.json"

NORMAL_SETS: Set[str] = {
    "expansion",
    "core",
    "draft_innovation",
    "commander",
    "masters",
}


def retryable_session(session: requests.Session, retries: int = 8) -> requests.Session:
    """
    Session with requests to allow for re-attempts at downloading missing data
    :param session: Session to download with
    :param retries: How many retries to attempt
    :return: Session that does downloading
    """
    retry = urllib3.util.retry.Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
    )

    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_generic_session() -> requests.Session:
    """Get or create a requests session for gatherer."""
    if mtgjson4.USE_CACHE.get():
        requests_cache.install_cache(
            str(mtgjson4.PROJECT_CACHE_PATH.joinpath("general_cache")),
            expire_after=mtgjson4.SESSION_CACHE_EXPIRE_GENERAL,
        )

    session: Optional[requests.Session] = SESSION.get(None)
    if not session:
        session = requests.Session()
        session = retryable_session(session)
        SESSION.set(session)

    return session


def is_number(string: str) -> bool:
    """See if a given string is a number (int or float)"""
    try:
        float(string)
        return True
    except ValueError:
        pass

    try:
        unicodedata.numeric(string)
        return True
    except (TypeError, ValueError):
        pass

    return False


def win_os_fix(set_name: str) -> str:
    """
    In the Windows OS, there are certain file names that are not allowed.
    In case we have a set with such a name, we will add a _ to the end to allow its existence
    on Windows.
    :param set_name: Set name
    :return: Set name with a _ if necessary
    """
    if set_name in mtgjson4.BANNED_FILE_NAMES:
        return set_name + "_"

    return set_name


def capital_case_without_symbols(name: str) -> str:
    """
    Determine the name of the output file by stripping
    all special characters and capital casing the words.
    :param name: Deck name (unsanitized)
    :return: Sanitized deck name
    """
    word_characters_only_regex = re.compile(r"[^\w]")
    capital_case = "".join(x for x in name.title() if not x.isspace())

    return word_characters_only_regex.sub("", capital_case)


def get_mtgjson_set_code(set_code: str) -> str:
    """
    Some set codes are wrong, so this will sanitize
    the set_code passed in
    :param set_code: Set code (unsanitized)
    :return: Sanitized set code
    """
    with mtgjson4.RESOURCE_PATH.joinpath("gatherer_set_codes.json").open(
        encoding="utf-8"
    ) as f:
        json_dict = json.load(f)
        for key, value in json_dict.items():
            if set_code == value:
                return str(key)

    return set_code


def print_download_status(response: Any) -> None:
    """
    When a file is downloaded, this will log that response
    :param response: Response
    """
    cache_result: bool = response.from_cache if hasattr(
        response, "from_cache"
    ) else False
    LOGGER.info(f"Downloaded: {response.url} (Cache = {cache_result})")


def url_keygen(prod_id: int) -> str:
    """
    Generates a key that MTGJSON will use for redirection
    :param prod_id: Seed
    :return: URL Key
    """
    return hashlib.sha256(str(prod_id).encode()).hexdigest()[:16]


def get_standard_sets() -> List[str]:
    """
    Use whatsinstandard to determine all sets that are legal in
    the standard format.
    :return: Standard legal set codes
    """
    if not STANDARD_SETS.get(None):
        # Get all sets currently in standard
        standard_url_content = get_generic_session().get(STANDARD_API_URL)
        standard_json = [
            set_obj["code"].upper()
            for set_obj in json.loads(standard_url_content.text)["sets"]
            if str(set_obj["enter_date"])
            < datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            < str(set_obj["exit_date"])
        ]
        STANDARD_SETS.set(standard_json)

    return list(STANDARD_SETS.get())


def strip_bad_sf_chars(bad_text: str) -> str:
    """
    Since we're searching Scryfall via name and not set code, we will
    have to strip the names to the bare minimums to get a valid result
    back.
    """
    for bad_char in [" ", ":", "'", "’", ".", "&"]:
        bad_text = bad_text.replace(bad_char, "")

    return bad_text


def get_gist_json_file(db_url: str, file_name: str) -> Any:
    """
    Grab the contents from a gist file
    :param db_url: Database URL
    :param file_name: File to open from Gist
    :return: File content
    """
    LOGGER.info("Cloning gist database")
    git_sh = git.cmd.Git()
    git_sh.clone(db_url, temp_working_dir, depth=1)

    with lzma.open(temp_working_dir.joinpath(file_name)) as file:
        return json.load(file)


def set_gist_json_file(
    username: str, api_token: str, repo_key: str, file_name: str, content: Any
) -> None:
    """
    Update a gist file and push it live
    :param username: GH api username
    :param api_token: GH api token
    :param repo_key: GH repo key
    :param file_name: File name
    :param content: New file content
    """
    with lzma.open(temp_working_dir.joinpath(file_name), "w") as file:
        file.write(json.dumps(content).encode("utf-8"))

    try:
        repo = git.Repo(temp_working_dir)

        # Update remote to allow pushing
        repo.git.remote(
            "set-url",
            "origin",
            f"https://{username}:{api_token}@gist.github.com/{repo_key}.git",
        )

        repo.git.commit("-am", "auto-push")
        origin = repo.remote()
        origin.push()
        LOGGER.info("Committing changes to CH database")
    except git.GitCommandError:
        LOGGER.warning("No changes to CH database detected")

    LOGGER.info("Removing local CH database")
    shutil.rmtree(temp_working_dir)


def build_format_map(
    all_sets: Dict[str, Any], regular: bool = True
) -> Dict[str, List[str]]:
    """
    For each set in the specified JSON file, determine its legal sets and return a dictionary mapping set code to
    a list of legal formats.
    :param all_sets: AllSets content
    :param regular: If this is True, then unusual sets will be excluded.
    :return: Dictionary of the form { format: [codes] }
    """
    formats: Dict[str, List[Any]] = {
        fmt: [] for fmt in mtgjson4.SUPPORTED_FORMAT_OUTPUTS
    }

    for code, data in all_sets.items():
        if regular and data["type"] not in NORMAL_SETS:
            continue

        possible_formats = mtgjson4.SUPPORTED_FORMAT_OUTPUTS

        for card in data.get("cards"):
            # The legalities dictionary only has keys for formats where the card is legal, banned or restricted.
            card_formats = set(card.get("legalities").keys())
            possible_formats = possible_formats.intersection(card_formats)

        for fmt in possible_formats:
            formats[fmt].append(code)

    return formats


def deep_merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Any:
    """
    Merge two dicts together, recursively
    :param dict1: Dict 1
    :param dict2: Dict 2
    :return: New Dict
    """
    for k in set(dict1.keys()).union(dict2.keys()):
        if k in dict1 and k in dict2:
            if isinstance(dict1[k], dict) and isinstance(dict2[k], dict):
                yield (k, dict(deep_merge_dicts(dict1[k], dict2[k])))
            else:
                yield (k, dict2[k])
        elif k in dict1:
            yield (k, dict1[k])
        else:
            yield (k, dict2[k])


def get_tcgplayer_to_mtgjson_map(all_printings_path: pathlib.Path) -> Dict[str, str]:
    """
    Generate a TCGPlayerID -> MTGJSON UUID map that can be used
    across the system.
    :param all_printings_path: Path to JSON compiled version
    :return: Map of TCGPlayerID -> MTGJSON UUID
    """
    with all_printings_path.open() as f:
        file_contents = json.load(f)

    dump_map: Dict[str, str] = {}
    for value in file_contents.values():
        for card in value.get("cards") + value.get("tokens"):
            if "tcgplayerProductId" in card.keys():
                dump_map[card["tcgplayerProductId"]] = card["uuid"]

    return dump_map
