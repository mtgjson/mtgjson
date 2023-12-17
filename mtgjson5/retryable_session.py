"""
Retryable Session to download content
"""
import datetime
import functools
import inspect
from typing import Union

import requests
import requests.adapters
import requests_cache
import urllib3

from . import constants
from .mtgjson_config import MtgjsonConfig


def retryable_session(
    retries: int = 8,
) -> Union[requests.Session, requests_cache.CachedSession]:
    """
    Session with requests to allow for re-attempts at downloading missing data
    :param retries: How many retries to attempt
    :return: Session that does the downloading
    """
    session: Union[requests.Session, requests_cache.CachedSession]

    if MtgjsonConfig().use_cache:
        stack = inspect.stack()
        calling_class = stack[1][0].f_locals["self"].__class__.__name__
        session = requests_cache.CachedSession(
            cache_name=str(constants.CACHE_PATH.joinpath(calling_class)),
            expire_after=datetime.timedelta(days=1),
            stale_if_error=True,
        )
    else:
        session = requests.Session()

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
    session.request = functools.partial(session.request, timeout=5)  # type: ignore

    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; +https://www.mtgjson.com) Gecko/20100101 Firefox/120.0"
        }
    )
    return session
