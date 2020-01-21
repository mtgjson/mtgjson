"""
Construct Prices for MTGJSON
"""
from typing import Dict, Any

import simplejson as json

from .consts import OUTPUT_PATH
from .providers import CardhoarderProvider, TCGPlayerProvider
from .utils import get_thread_logger

LOGGER = get_thread_logger()


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
