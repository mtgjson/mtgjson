"""MTGJSON Structures compiled model for output file definitions."""

from typing import List

from ..mtgjson_base import MTGJsonCompiledModel


class MtgjsonStructuresObject(MTGJsonCompiledModel):
    """
    MTGJSON Internal Object for Output Files
    Note: Singleton pattern removed for Pydantic compatibility
    """

    all_printings: str = "AllPrintings"
    atomic_cards: str = "AtomicCards"
    all_prices: str = "AllPrices"
    all_prices_today: str = "AllPricesToday"
    all_csvs_directory: str = "AllPrintingsCSVFiles"
    all_parquets_directory: str = "AllPrintingsParquetFiles"
    all_decks_directory: str = "AllDeckFiles"
    all_sets_directory: str = "AllSetFiles"
    card_types: str = "CardTypes"
    compiled_list: str = "CompiledList"
    deck_list: str = "DeckList"
    key_words: str = "Keywords"
    enum_values: str = "EnumValues"
    set_list: str = "SetList"
    referral_database: str = "ReferralMap"
    version: str = "Meta"
    all_identifiers: str = "AllIdentifiers"
    all_tcgplayer_skus: str = "TcgplayerSkus"
    all_printings_standard: str = "Standard"
    all_printings_pioneer: str = "Pioneer"
    all_printings_modern: str = "Modern"
    all_printings_legacy: str = "Legacy"
    all_printings_vintage: str = "Vintage"
    atomic_cards_standard: str = "StandardAtomic"
    atomic_cards_pioneer: str = "PioneerAtomic"
    atomic_cards_modern: str = "ModernAtomic"
    atomic_cards_legacy: str = "LegacyAtomic"
    atomic_cards_vintage: str = "VintageAtomic"
    atomic_cards_pauper: str = "PauperAtomic"

    def get_all_compiled_file_names(self) -> List[str]:
        """
        Get all files that are compiled outputs
        :return: Compiled outputs files
        """
        return list(set(self.model_dump().values()))

    def get_compiled_list_files(self) -> List[str]:
        """
        Get all files that should appear in CompiledList.json
        :return: Files for CompiledList.json
        """
        return list(set(self.get_all_compiled_file_names()) - {self.referral_database})
