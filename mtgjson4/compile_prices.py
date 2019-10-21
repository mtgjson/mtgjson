"""
Tool to generate prices from all sources
"""
import json
import logging
import multiprocessing
import pathlib
from typing import Any, Dict, List, Tuple, Union

import mtgjson4
from mtgjson4.provider import cardhoader, mtgstocks

LOGGER = logging.getLogger(__name__)


def build_price_data(card: Dict[str, Any]) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """
    Build up price info for a single card and add it to global index
    :param card: Card to get price data of
    :return Card object
    """
    LOGGER.info(f"Building price for {card['name']}")
    paper_pricing = mtgstocks.get_card_data(card.get("tcgplayerProductId", -1))
    digital_pricing = cardhoader.get_card_data(card["uuid"])

    return (
        card["uuid"],
        {
            "prices": {
                "paper": paper_pricing.get("paper", {}),
                "paperFoil": paper_pricing.get("foil", {}),
                "mtgo": digital_pricing.get("mtgo", {}),
                "mtgoFoil": digital_pricing.get("mtgoFoil", {}),
            }
        },
    )


class MtgjsonPrice:
    """
    Class to construct MTGJSON Pricing data for additional download files.
    """

    def __init__(self, all_sets_path: Union[str, pathlib.Path]) -> None:
        """
        Initializer to load in cards and establish pricing database
        :param all_sets_path: Path to AllSets.json
        """
        self.mtgjson_cards: List[Dict[str, Any]] = []
        self.prices_output: Dict[str, Dict[str, Dict[str, str]]] = {}

        self.all_sets_path = pathlib.Path(all_sets_path).expanduser()
        if not self.all_sets_path.exists():
            LOGGER.error(f"Pricing can't find AllSets at {self.all_sets_path}")
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
        Load in all MTGJSON cards from AllSets.json file
        """
        with self.all_sets_path.expanduser().open() as file:
            all_sets = json.load(file)

        for set_content in all_sets.values():
            self.mtgjson_cards.extend(set_content.get("cards", []))

    @staticmethod
    def __prime_databases() -> None:
        """
        Prime price databases before multiprocessing iterations
        """
        mtgstocks.get_card_data(0)
        cardhoader.get_card_data("")

    def __collate_pricing(self) -> None:
        """
        Build up price databases in parallel
        """
        LOGGER.info("Priming Database")
        self.__prime_databases()

        LOGGER.info("Starting Pool")
        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            futures = pool.map_async(build_price_data, self.mtgjson_cards)
            pool.close()
            pool.join()

        for card_price in futures.get():
            self.prices_output[card_price[0]] = card_price[1]
