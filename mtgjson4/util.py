"""Utility functions."""
import contextvars
import json
import re
from typing import Optional

import requests
import requests.adapters
import urllib3.util.retry

import mtgjson4

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")


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
    session: Optional[requests.Session] = SESSION.get(None)
    if session is None:
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
        import unicodedata

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
        "r", encoding="utf-8"
    ) as f:
        json_dict = json.load(f)
        for key, value in json_dict.items():
            if set_code == value:
                return str(key)

    return set_code
