"""
Decks via GitHub 3rd party provider
"""
import copy
import json
import logging
import pathlib
from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Union

from singleton_decorator import singleton

from ..classes import MtgjsonSetDeckObject
from ..classes.mtgjson_deck import MtgjsonDeckObject
from ..compiled_classes.mtgjson_structures import MtgjsonStructuresObject
from ..mtgjson_config import MtgjsonConfig
from ..providers.abstract import AbstractProvider
from ..utils import parallel_call, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubDecksProvider(AbstractProvider):
    """
    GitHubDecksProvider container
    """

    decks_api_url: str = "https://github.com/taw/magic-preconstructed-decks-data/blob/master/decks_v2.json?raw=true"
    decks_uuid_api_url: str = "https://github.com/mtgjson/mtg-sealed-content/blob/main/outputs/deck_map.json?raw=True"
    all_printings_file: pathlib.Path = MtgjsonConfig().output_path.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    )
    all_printings_cards: Dict[str, Any]
    decks_by_set: Dict[str, List[MtgjsonSetDeckObject]]

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.decks_by_set = defaultdict(list)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return {}

    def get_decks_in_set(self, set_code: str) -> List[MtgjsonSetDeckObject]:
        """
        Get individual decks within a specific set, from cache
        Builds up cache if not set
        :param set_code Set code to get decks for
        :return Decks in set code
        """
        if not self.decks_by_set:
            decks_uuid_content = self.download(self.decks_uuid_api_url)
            for deck in self.download(self.decks_api_url):
                sealed_uuids = decks_uuid_content.get(set_code.lower(), {}).get(
                    deck["name"]
                )
                mtgjson_set_deck = MtgjsonSetDeckObject(deck["name"], sealed_uuids)

                for card in deck.get("cards", []):
                    mtgjson_set_deck.cards.append(
                        MtgjsonSetDeckObject.MtgjsonSetDeckCardObject(
                            card["mtgjson_uuid"],
                            card["count"],
                            "foil" if card["foil"] else "nonfoil",
                        )
                    )

                self.decks_by_set[deck.get("set_code").upper()].append(mtgjson_set_deck)

        return self.decks_by_set.get(set_code, [])

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
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
            cards.append(copy.deepcopy(mtgjson_card))

    if not cards:
        LOGGER.warning(f"No matches found for {card}")

    return cards
