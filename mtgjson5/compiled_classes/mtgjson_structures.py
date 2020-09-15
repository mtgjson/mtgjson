"""
MTGJSON Internal Object for Output Files
"""
from typing import Any, Dict, List

from singleton_decorator import singleton

from ..utils import to_camel_case


@singleton
class MtgjsonStructuresObject:
    """
    MTGJSON Internal Object for Output Files
    """

    all_printings: str
    atomic_cards: str
    all_prices: str

    all_decks_directory: str
    all_sets_directory: str

    card_types: str
    compiled_list: str
    deck_list: str
    key_words: str
    set_list: str

    enum_values: str

    referral_database: str
    version: str

    all_identifiers: str

    all_tcgplayer_skus: str

    all_printings_standard: str
    all_printings_pioneer: str
    all_printings_modern: str
    all_printings_legacy: str
    all_printings_vintage: str

    atomic_cards_standard: str
    atomic_cards_pioneer: str
    atomic_cards_modern: str
    atomic_cards_legacy: str
    atomic_cards_vintage: str
    atomic_cards_pauper: str

    def __init__(self) -> None:
        """
        Initializer to build up the object
        """
        self.all_printings = "AllPrintings"
        self.atomic_cards = "AtomicCards"
        self.all_prices = "AllPrices"
        self.all_csvs_directory = "AllPrintingsCSVFiles"
        self.all_decks_directory = "AllDeckFiles"
        self.all_sets_directory = "AllSetFiles"
        self.card_types = "CardTypes"
        self.compiled_list = "CompiledList"
        self.deck_list = "DeckList"
        self.key_words = "Keywords"
        self.enum_values = "EnumValues"
        self.set_list = "SetList"
        self.referral_database = "ReferralMap"
        self.version = "Meta"
        self.all_identifiers = "AllIdentifiers"
        self.all_tcgplayer_skus = "TcgplayerSkus"
        self.all_printings_standard = "Standard"
        self.all_printings_pioneer = "Pioneer"
        self.all_printings_modern = "Modern"
        self.all_printings_legacy = "Legacy"
        self.all_printings_vintage = "Vintage"
        self.atomic_cards_standard = "StandardAtomic"
        self.atomic_cards_pioneer = "PioneerAtomic"
        self.atomic_cards_modern = "ModernAtomic"
        self.atomic_cards_legacy = "LegacyAtomic"
        self.atomic_cards_vintage = "VintageAtomic"
        self.atomic_cards_pauper = "PauperAtomic"

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

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
