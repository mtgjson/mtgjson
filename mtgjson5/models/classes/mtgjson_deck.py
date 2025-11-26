"""MTGJSON Deck Object model for pre-constructed and user deck data."""

import re
from typing import Any

from pydantic import Field, PrivateAttr, model_validator

from ..mtgjson_base import MTGJsonModel
from .mtgjson_card import MtgjsonCardObject
from .mtgjson_sealed_product import MtgjsonSealedProductObject


class MtgjsonDeckObject(MTGJsonModel):
    """
    The Deck Data Model describes the properties of an individual Deck.
    """

    code: str = Field(default="", description="The printing set code for the deck.")
    commander: list[MtgjsonCardObject | dict[str, Any]] = Field(
        default_factory=list, description="The card that is the Commander in this deck."
    )
    main_board: list[MtgjsonCardObject | dict[str, Any]] = Field(
        default_factory=list, description="The cards in the main-board."
    )
    name: str = Field(default="", description="The name of the deck.")
    release_date: str | None = Field(
        default=None, description="The release date in ISO 8601 format for the set."
    )
    sealed_product_uuids: list[str] | None = Field(
        default=None,
        description="A cross-reference identifier to determine which sealed products contain this deck.",
    )
    side_board: list[MtgjsonCardObject | dict[str, Any]] = Field(
        default_factory=list, description="The cards in the side-board."
    )
    tokens: list[MtgjsonCardObject | dict[str, Any]] = Field(
        default_factory=list, description="The tokens included with the product."
    )
    type: str = Field(default="", description="The type of deck.")

    # Extended fields not in schema.py but used internally
    display_commander: list[MtgjsonCardObject | dict[str, Any]] = Field(
        default_factory=list
    )
    planes: list[MtgjsonCardObject | dict[str, Any]] = Field(default_factory=list)
    schemes: list[MtgjsonCardObject | dict[str, Any]] = Field(default_factory=list)
    file_name: str = ""
    source_set_codes: list[str] = Field(default_factory=list)

    # Private field (excluded from serialization)
    _alpha_numeric_name: str = PrivateAttr(default="")

    @model_validator(mode="after")
    def set_alpha_numeric_name(self) -> "MtgjsonDeckObject":
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
        self, mtgjson_set_sealed_products: list[MtgjsonSealedProductObject]
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

    def build_keys_to_skip(self) -> set[str]:
        """
        Keys to exclude from JSON output
        :return: Set of keys to skip
        """
        return {"file_name"}
