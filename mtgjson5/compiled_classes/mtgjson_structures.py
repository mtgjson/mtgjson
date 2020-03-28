"""
MTGJSON container that holds what output files should be generated
"""
from typing import Any, Dict, List, Set

from singleton_decorator import singleton

from ..utils import to_camel_case


@singleton
class MtgjsonStructuresObject:
    """
    MTGJSON's container for output files
    """

    all_printings: str
    all_cards: str
    all_prices: str

    all_decks_directory: str
    all_sets_directory: str

    card_types: str
    compiled_list: str
    deck_lists: str
    key_words: str
    set_list: str

    referral_database: str
    version: str

    all_printings_standard: str
    all_printings_pioneer: str
    all_printings_modern: str
    all_printings_legacy: str
    all_printings_vintage: str

    all_cards_standard: str
    all_cards_pioneer: str
    all_cards_modern: str
    all_cards_legacy: str
    all_cards_vintage: str
    all_cards_pauper: str

    def __init__(self) -> None:
        self.all_printings: str = "AllPrintings"
        self.all_cards: str = "AllCards"
        self.all_prices: str = "AllPrices"
        self.all_decks_directory: str = "AllDeckFiles"
        self.all_sets_directory: str = "AllSetFiles"
        self.card_types: str = "CardTypes"
        self.compiled_list: str = "CompiledList"
        self.deck_lists: str = "DeckLists"
        self.key_words: str = "Keywords"
        self.set_list: str = "SetList"
        self.referral_database = "ReferralMap"
        self.version: str = "Meta"
        self.all_printings_standard: str = "StandardPrintings"
        self.all_printings_pioneer: str = "PioneerPrintings"
        self.all_printings_modern: str = "ModernPrintings"
        self.all_printings_legacy: str = "LegacyPrintings"
        self.all_printings_vintage: str = "VintagePrintings"
        self.all_cards_standard: str = "StandardCards"
        self.all_cards_pioneer: str = "PioneerCards"
        self.all_cards_modern: str = "ModernCards"
        self.all_cards_legacy: str = "LegacyCards"
        self.all_cards_vintage: str = "VintageCards"
        self.all_cards_pauper: str = "PauperCards"

    def get_all_compiled_file_names(self) -> List[str]:
        """
        Get all files that are compiled outputs
        :return: Compiled outputs files
        """
        return list(set(self.__dict__.values()))

    def get_compiled_list_files(self) -> List[str]:
        """
        Get all files that should appear in CompiledList.json
        :return: Files for CompiledList.json
        """
        return list(set(self.get_all_compiled_file_names()) - {self.referral_database})

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys: Set[str] = set()

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and key not in skip_keys
        }
