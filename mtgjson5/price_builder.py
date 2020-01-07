"""
Construct Prices for MTGJSON
"""
import simplejson as json

from .providers import CardhoarderProvider
from .utils import get_thread_logger

LOGGER = get_thread_logger()


def build_prices() -> None:
    """

    :return:
    """
    cardhoarder_prices = CardhoarderProvider().generate_today_price_dict()
    LOGGER.info(json.dumps(cardhoarder_prices, indent=4))
