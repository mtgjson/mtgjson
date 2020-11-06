"""
Decks via GitHub 3rd party provider
"""
import json
import logging
import pathlib
from typing import Any, Dict, Iterator, List, Union

from singleton_decorator import singleton

from ..classes.mtgjson_deck import MtgjsonDeckObject
from ..compiled_classes.mtgjson_structures import MtgjsonStructuresObject
from ..consts import OUTPUT_PATH
from ..providers.abstract import AbstractProvider
from ..utils import parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubDecksProvider(AbstractProvider):
    """
    GitHubDecksProvider container
    """

    decks_api_url: str = "https://github.com/taw/magic-preconstructed-decks-data/blob/master/decks_v2.json?raw=true"
    all_printings_file: pathlib.Path = OUTPUT_PATH.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    )
    all_printings_cards: Dict[str, Any]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return dict()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        session = retryable_session()

        response = session.get(url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(f"Error downloading GitHub Decks: {response} --- {response.text}")
        return []

    def iterate_precon_decks(self) -> Iterator[MtgjsonDeckObject]:
        """
        Iterate the pre-constructed headers file to generate
        full MTGJSON deck objects
        :return: Iterator of a deck object
        """
        if not self.all_printings_file.is_file():
            LOGGER.error("Unable to construct decks. AllPrintings not found")
            return

        if self.all_printings_file.stat().st_size <= 2000:
            LOGGER.error("Unable to construct decks. AllPrintings not fully formed")
            return

        with self.all_printings_file.open(encoding="utf-8") as file:
            self.all_printings_cards = json.load(file).get("data", {})

        for deck in self.download(self.decks_api_url):
            this_deck = MtgjsonDeckObject()
            this_deck.name = deck["name"]
            this_deck.code = deck["set_code"].upper()
            this_deck.set_sanitized_name(this_deck.name)
            this_deck.type = deck["type"]
            this_deck.release_date = deck["release_date"]

            try:
                this_deck.main_board = parallel_call(
                    build_single_card, deck["cards"], fold_list=True
                )
                this_deck.side_board = parallel_call(
                    build_single_card, deck["sideboard"], fold_list=True
                )
                this_deck.commander = parallel_call(
                    build_single_card, deck["commander"], fold_list=True
                )
            except KeyError as error:
                LOGGER.warning(
                    f'GitHub Deck "{this_deck.name}" failed to build -- Missing Set {error}'
                )
                continue

            yield this_deck


def build_single_card(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Given a card, add components necessary to turn it into
    an enhanced MTGJSON card
    :param card: Card to enhance
    :return: List of enhanced cards in set
    """
    cards = []
    set_to_build_from = GitHubDecksProvider().all_printings_cards.get(
        card["set_code"].upper()
    )

    if not set_to_build_from:
        LOGGER.warning(f"Set {card['set_code'].upper()} not found for {card['name']}")
        return []

    for mtgjson_card in set_to_build_from["cards"]:
        if card["mtgjson_uuid"] == mtgjson_card["uuid"]:
            mtgjson_card["count"] = card["count"]
            mtgjson_card["isFoil"] = card["foil"]
            cards.append(mtgjson_card)

    if not cards:
        LOGGER.warning(f"No matches found for {card}")

    return cards
