"""
Tool to generate prices from all sources
"""
import json
import logging
import multiprocessing
import pathlib
from typing import Any, Dict, List, Tuple, Union

import mtgjson4
from mtgjson4.provider import cardhoader, tcgplayer

LOGGER = logging.getLogger(__name__)


def build_price_data(card: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """
    Build up price info for a single card and add it to global index
    :param card: Card to get price data of
    :return Card object
    """
    LOGGER.info(f"Building price for {card['name']}")
    return card["uuid"], {"prices": cardhoader.get_card_data(card["uuid"])}


class MtgjsonPrice:
    """
    Class to construct MTGJSON Pricing data for additional download files.
    """

    def __init__(self, all_printings_path: Union[str, pathlib.Path]) -> None:
        """
        Initializer to load in cards and establish pricing database
        :param all_printings_path: Path to AllPrintings, without ending (needs JSON and SQLITE there)
        """
        self.mtgjson_cards: List[Dict[str, Any]] = []
        self.prices_output: Dict[str, Dict[str, Dict[str, str]]] = {}

        self.all_printings_path = (
            pathlib.Path(all_printings_path).expanduser().with_suffix(".json")
        )
        if not self.all_printings_path.exists():
            LOGGER.error(
                f"Pricing can't find AllPrintings.json at {self.all_printings_path}"
            )
            return

        self.all_printings_sqlite_path = (
            pathlib.Path(all_printings_path).expanduser().with_suffix(".sqlite")
        )
        if not self.all_printings_path.exists():
            LOGGER.error(
                f"Pricing can't find AllPrintings.sqlite at {self.all_printings_path}"
            )
            return

        self.__load_mtgjson_cards_from_file()
        self.__collate_pricing()

    def __bool__(self) -> bool:
        """
        See if the class has been properly initialized
        :return: Class initialization status
        """
        return bool(self.prices_output)

    def get_price_database(self) -> str:
        """
        Get price data dumps for output files
        :return: Price database
        """
        return json.dumps(
            self.prices_output, sort_keys=True, indent=mtgjson4.PRETTY_OUTPUT.get()
        )

    def __load_mtgjson_cards_from_file(self) -> None:
        """
        Load in all MTGJSON cards from AllPrintings.json file
        """
        with self.all_printings_path.expanduser().open() as file:
            all_sets = json.load(file)

        for set_content in all_sets.values():
            self.mtgjson_cards.extend(set_content.get("cards", []))

    def __prime_databases(self) -> None:
        """
        Prime price databases before multiprocessing iterations
        This adds values from _now_ to the database
        """
        tcgplayer.generate_and_store_tcgplayer_prices(
            str(self.all_printings_sqlite_path)
        )
        cardhoader.get_card_data("")

    def __collate_pricing(self) -> None:
        """
        Build up price databases in parallel
        """
        LOGGER.info("Priming Database")
        self.__prime_databases()

        LOGGER.info("Starting Pool")
        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            futures = pool.map(build_price_data, self.mtgjson_cards)

        for card_price in futures:
            self.prices_output[card_price[0]] = card_price[1]
