"""
Functions and constants used to help build the format map
"""
from typing import List, Dict, Any, Set

from mtgjson4 import SUPPORTED_FORMAT_OUTPUTS

NORMAL_SETS: Set[str] = {
    "expansion",
    "core",
    "draft_innovation",
    "commander",
    "masters",
}


def build_format_map(
    all_sets: Dict[str, Any], regular: bool = True
) -> Dict[str, List[str]]:
    """
    For each set in the specified JSON file, determine its legal sets and return a dictionary mapping set code to
    a list of legal formats.

    :param all_sets: AllSets content
    :type all_sets: dict
    :param regular: If this is True, then unusual sets will be excluded.
    :type regular: bool

    :return: Dictionary of the form { format: [codes] }
    :rtype: dict
    """
    formats: Dict[str, List[Any]] = {fmt: [] for fmt in SUPPORTED_FORMAT_OUTPUTS}

    for code, data in all_sets.items():
        if regular and data["type"] not in NORMAL_SETS:
            continue

        possible_formats = set(SUPPORTED_FORMAT_OUTPUTS)

        for card in data.get("cards"):
            # The legalities dictionary only has keys for formats where the card is legal, banned or restricted.
            card_formats = set(card.get("legalities").keys())
            possible_formats = possible_formats.intersection(card_formats)

        for fmt in possible_formats:
            formats[fmt].append(code)

    return formats
