"""Utility functions."""
import contextvars
from typing import Optional

import requests
import requests.adapters
import urllib3.util.retry

SESSION: contextvars.ContextVar = contextvars.ContextVar("SESSION")


def retryable_session(session: requests.Session, retries: int = 5) -> requests.Session:
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
