import re
from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import Field, PrivateAttr, model_validator

from ..mtgjson_base import MTGJsonModel
from .mtgjson_card import MtgjsonCardObject
from .mtgjson_sealed_product import MtgjsonSealedProductObject


class MtgjsonDeckObject(MTGJsonModel):
    """
    MTGJSON Singular Deck Object
    """

    main_board: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    side_board: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    display_commander: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    commander: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    planes: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    schemes: List[Union[MtgjsonCardObject, Dict[str, Any]]] = Field(default_factory=list)
    code: str = ""
    name: str = ""
    release_date: str = ""
    type: str = ""
    file_name: str = ""
    sealed_product_uuids: Optional[List[str]] = None

    # Private field (excluded from serialization)
    _alpha_numeric_name: str = PrivateAttr(default="")

    @model_validator(mode='after')
    def set_alpha_numeric_name(self):
        """Set sanitized name after initialization."""
        if self.name:
            self._alpha_numeric_name = re.sub(r"[^A-Za-z0-9 ]+", "", self.name).lower()
        return self

    def set_sanitized_name(self, name: str) -> None:
        """
        Turn an unsanitary file name to a safe one
        :param name: Unsafe name
        """
        word_characters_only_regex = re.compile(r"\W")
        capital_case = "".join(x for x in name.title() if not x.isspace())
        deck_name_sanitized = word_characters_only_regex.sub("", capital_case)
        self.file_name = f"{deck_name_sanitized}_{self.code}"

    def add_sealed_product_uuids(
        self, mtgjson_set_sealed_products: List[MtgjsonSealedProductObject]
    ) -> None:
        """
        Update the UUID for the deck to link back to sealed product, if able
        :param mtgjson_set_sealed_products: MTGJSON Set Sealed Products for this Set
        """
        if not self.sealed_product_uuids:
            for sealed_product_entry in mtgjson_set_sealed_products:
                sealed_name = sealed_product_entry.name.lower()
                if self._alpha_numeric_name in sealed_name:
                    self.sealed_product_uuids = [sealed_product_entry.uuid]
                    break

    def build_keys_to_skip(self) -> Iterable[str]:
        """
        Keys to exclude from JSON output
        :return: Set of keys to skip
        """
        return {"file_name"}
