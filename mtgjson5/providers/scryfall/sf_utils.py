"""Utility functions for Scryfall provider HTTP configuration."""

import logging

from ...mtgjson_config import MtgjsonConfig

LOGGER = logging.getLogger(__name__)


def build_http_header() -> dict[str, str]:
    """
    Construct the Authorization header for Scryfall
    :return: Authorization header
    """
    if not MtgjsonConfig().has_section("Scryfall"):
        LOGGER.warning("Scryfall section not established. Defaulting to non-authorized mode")
        return {}

    if not MtgjsonConfig().has_option("Scryfall", "client_secret"):
        LOGGER.warning("Scryfall keys values missing. Defaulting to non-authorized mode")
        return {}

    headers: dict[str, str] = {
        "Authorization": f"Bearer {MtgjsonConfig().get('Scryfall', 'client_secret')}",
        "Connection": "Keep-Alive",
    }
    return headers
