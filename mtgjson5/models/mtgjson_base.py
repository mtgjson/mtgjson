"""Base Pydantic models for MTGJSON object serialization and deserialization."""

import re
import datetime
from typing import Any, Callable, ClassVar, Dict, Set

from pydantic_core import core_schema
from pydantic.alias_generators import to_camel
from pydantic import (field_serializer, computed_field,
    BaseModel, ConfigDict, Field, model_serializer
)

_CAMEL_TO_SNAKE_1 = re.compile(r"(.)([A-Z][a-z]+)")
_CAMEL_TO_SNAKE_2 = re.compile(r"([a-z0-9])([A-Z])")

class MTGJsonModel(BaseModel):
    """
    Base for all MTGJSON models with custom serialization logic.
    """

    @field_serializer("*", when_used="json", check_fields=False)
    def serialize_dates_and_sets(self, value: Any) -> Any:
        """field serializer for datetime and set types."""
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, set):
            return list(value)
        return value

    def build_keys_to_skip(self) -> Set[str]:
        """Override to define fields to exclude dynamically."""
        return set()

    @model_serializer(mode="wrap")
    def serialize_model(
        self,
        serializer: Callable[[Any], Dict[str, Any]],
        _info: core_schema.SerializationInfo,
    ) -> Dict[str, Any]:
        """Custom serialization respecting build_keys_to_skip()."""
        data = serializer(self)
        skip_keys = self.build_keys_to_skip()

        if not skip_keys:
            return data

        result = {}
        for field_name, value in data.items():
            snake_case_field = self._to_snake_case(field_name)
            if snake_case_field not in skip_keys:
                result[field_name] = value

        return result

    @staticmethod
    def _to_snake_case(camel_str: str) -> str:
        """regex camelCase -> snake_case conversion."""
        # module level regex compilation for a little speed boost
        s1 = _CAMEL_TO_SNAKE_1.sub(r"\1_\2", camel_str)
        return _CAMEL_TO_SNAKE_2.sub(r"\1_\2", s1).lower()

    def to_json(self) -> Dict[str, Any]:
        """Backward compatibility with existing to_json() calls."""
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra='ignore',
        use_enum_values=True,
        validate_assignment=True,
        validate_default=False,
        revalidate_instances="never",
        from_attributes=True,
    )

class MTGJsonCardBase(MTGJsonModel):
    """
    Extended Base for all MTGJSON Card models with dynamic field exclusion.
    """

    _allow_if_falsey: ClassVar[Set[str]] = {
        "supertypes",
        "types",
        "subtypes",
        "has_foil",
        "has_non_foil",
        "color_identity",
        "colors",
        "converted_mana_cost",
        "mana_value",
        "face_converted_mana_cost",
        "face_mana_value",
        "foreign_data",
        "reverse_related",
    }

    _exclude_for_tokens: ClassVar[Set[str]] = {
        "rulings",
        "rarity",
        "prices",
        "purchase_urls",
        "printings",
        "converted_mana_cost",
        "mana_value",
        "foreign_data",
        "legalities",
        "leadership_skills",
    }

    _exclude_for_cards: ClassVar[Set[str]] = {"reverse_related"}

    _atomic_keys: list[str] = [
        "ascii_name",
        "color_identity",
        "color_indicator",
        "colors",
        "converted_mana_cost",
        "count",
        "defense",
        "edhrec_rank",
        "edhrec_saltiness",
        "face_converted_mana_cost",
        "face_mana_value",
        "face_name",
        "foreign_data",
        "hand",
        "has_alternative_deck_limit",
        "identifiers",
        "is_funny",
        "is_reserved",
        "keywords",
        "layout",
        "leadership_skills",
        "legalities",
        "life",
        "loyalty",
        "mana_cost",
        "mana_value",
        "name",
        "power",
        "printings",
        "purchase_urls",
        "rulings",
        "scryfall_oracle_id",
        "side",
        "subtypes",
        "supertypes",
        "text",
        "toughness",
        "type",
        "types",
    ]

    uuid: str = Field(default="", exclude=False)
    is_token: bool = Field(default=False, exclude=True)

    def build_keys_to_skip(self) -> Set[str]:
        """Dynamic field exclusion for cards."""
        if self.is_token:
            excluded_keys = self._exclude_for_tokens.copy()
        else:
            excluded_keys = self._exclude_for_cards.copy()

        # Exclude empty fields unless specifically allowed
        for field_name, field_value in self.__dict__.items():
            if not field_value and field_name not in self._allow_if_falsey:
                excluded_keys.add(field_name)

        return excluded_keys
    
    def to_json(self) -> Dict[str, Any]:
        """
        Custom JSON serialization that filters out empty values
        :return: JSON object
        """
        skip_keys = self.build_keys_to_skip()
        return self.model_dump(by_alias=True, exclude=skip_keys, exclude_none=True, mode="json")
        
class MTGJsonSetModel(MTGJsonModel):
    """
    Extended Base for all MTGJSON Set models with custom Windows-safe set code.
    """

    _BAD_FILE_NAMES: ClassVar[Set[str]] = {"CON", "PRN", "AUX", "NUL", "COM1", "LPT1"}

    name: str = Field(default="")
    code: str = Field(default="")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def windows_set_code(self) -> str:
        """Set code safe for Windows filesystem.

        appends underscore to reserved names.
        computed property is automatically serialized and cached.
        """
        if self.code in self._BAD_FILE_NAMES:
            return self.code + "_"
        return self.code

    def get_windows_safe_set_code(self) -> str:
        """Handle Windows-incompatible file names.

        Deprecated: Use windows_safe_code property instead.
        Kept for backward compatibility.
        """
        return self.windows_set_code