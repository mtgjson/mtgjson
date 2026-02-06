"""
MTGJSON Singular Deck Object
"""

import re
from collections.abc import Iterable

from .json_object import JsonObject
from .mtgjson_card import MtgjsonCardObject
from .mtgjson_sealed_product import MtgjsonSealedProductObject


class MtgjsonDeckObject(JsonObject):
    """
    MTGJSON Singular Card Object
    """

    main_board: list[MtgjsonCardObject]
    side_board: list[MtgjsonCardObject]
    display_commander: list[MtgjsonCardObject]
    commander: list[MtgjsonCardObject]
    planes: list[MtgjsonCardObject]
    schemes: list[MtgjsonCardObject]
    tokens: list[MtgjsonCardObject]

    code: str
    name: str
    release_date: str
    sealed_product_uuids: list[str] | None
    source_set_codes: list[str]
    type: str
    file_name: str

    __alpha_numeric_name: str

    def __init__(
        self,
        deck_name: str = "",
        sealed_product_uuids: list[str] | None = None,
    ):
        self.name = deck_name
        self.sealed_product_uuids = sealed_product_uuids
        self.__alpha_numeric_name = re.sub(r"[^A-Za-z0-9 ]+", "", self.name).lower()
        self.main_board = []
        self.side_board = []
        self.display_commander = []
        self.commander = []
        self.planes = []
        self.schemes = []
        self.tokens = []

    def set_sanitized_name(self, name: str) -> None:
        """
        Turn an unsanitary file name to a safe one
        :param name: Unsafe name
        """
        word_characters_only_regex = re.compile(r"\W")
        capital_case = "".join(x for x in name.title() if not x.isspace())

        deck_name_sanitized = word_characters_only_regex.sub("", capital_case)

        self.file_name = f"{deck_name_sanitized}_{self.code}"

    def add_sealed_product_uuids(self, mtgjson_set_sealed_products: list[MtgjsonSealedProductObject]) -> None:
        """
        Update the UUID for the deck to link back to sealed product, if able
        :param mtgjson_set_sealed_products MTGJSON Set Sealed Products for this Set
        """
        if not self.sealed_product_uuids:
            for sealed_product_entry in mtgjson_set_sealed_products:
                sealed_name = sealed_product_entry.name.lower()
                if self.__alpha_numeric_name in sealed_name:
                    self.sealed_product_uuids = [sealed_product_entry.uuid]
                    break

    def build_keys_to_skip(self) -> Iterable[str]:
        return {"file_name"}
