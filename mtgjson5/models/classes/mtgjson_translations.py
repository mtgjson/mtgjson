"""MTGJSON Translations Object model for localized set names."""

from typing import Any, Dict, Optional

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonTranslationsObject(MTGJsonModel):
    """
    The Translations Data Model describes the properties of a Set or Set List's name translated in various alternate languages.
    """

    ancient_greek: str | None = Field(
        default=None,
        alias="Ancient Greek",
        description="The set name translation in Ancient Greek.",
    )
    arabic: str | None = Field(
        default=None, alias="Arabic", description="The set name translation in Arabic."
    )
    chinese_simplified: str | None = Field(
        default=None,
        alias="Chinese Simplified",
        description="The set name translation in Chinese Simplified.",
    )
    chinese_traditional: str | None = Field(
        default=None,
        alias="Chinese Traditional",
        description="The set name translation in Chinese Traditional.",
    )
    french: str | None = Field(
        default=None, alias="French", description="The set name translation in French."
    )
    german: str | None = Field(
        default=None, alias="German", description="The set name translation in German."
    )
    hebrew: str | None = Field(
        default=None, alias="Hebrew", description="The set name translation in Hebrew."
    )
    italian: str | None = Field(
        default=None,
        alias="Italian",
        description="The set name translation in Italian.",
    )
    japanese: str | None = Field(
        default=None,
        alias="Japanese",
        description="The set name translation in Japanese.",
    )
    korean: str | None = Field(
        default=None, alias="Korean", description="The set name translation in Korean."
    )
    latin: str | None = Field(
        default=None, alias="Latin", description="The set name translation in Latin."
    )
    phyrexian: str | None = Field(
        default=None,
        alias="Phyrexian",
        description="The set name translation in Phyrexian.",
    )
    portuguese_brazil: str | None = Field(
        default=None,
        alias="Portuguese (Brazil)",
        description="The set name translation in Portuguese (Brazil).",
    )
    russian: str | None = Field(
        default=None,
        alias="Russian",
        description="The set name translation in Russian.",
    )
    sanskrit: str | None = Field(
        default=None,
        alias="Sanskrit",
        description="The set name translation in Sanskrit.",
    )
    spanish: str | None = Field(
        default=None,
        alias="Spanish",
        description="The set name translation in Spanish.",
    )

    def __init__(
        self, active_dict: Optional[Dict[str, str]] = None, **data: Any
    ) -> None:
        """
        Initializer, for each language, given the contents
        :param active_dict: Dictionary mapping language names to translations
        """
        if active_dict:
            data.update(
                {
                    "ancient_greek": active_dict.get("Ancient Greek"),
                    "arabic": active_dict.get("Arabic"),
                    "chinese_simplified": active_dict.get("Chinese Simplified"),
                    "chinese_traditional": active_dict.get("Chinese Traditional"),
                    "french": active_dict.get("French", active_dict.get("fr")),
                    "german": active_dict.get("German", active_dict.get("de")),
                    "hebrew": active_dict.get("Hebrew"),
                    "italian": active_dict.get("Italian", active_dict.get("it")),
                    "japanese": active_dict.get("Japanese"),
                    "korean": active_dict.get("Korean"),
                    "latin": active_dict.get("Latin"),
                    "phyrexian": active_dict.get("Phyrexian"),
                    "portuguese_brazil": active_dict.get("Portuguese (Brazil)"),
                    "russian": active_dict.get("Russian"),
                    "sanskrit": active_dict.get("Sanskrit"),
                    "spanish": active_dict.get("Spanish", active_dict.get("es")),
                }
            )
        super().__init__(**data)

    @staticmethod
    def parse_key(key: str) -> str:
        """
        Custom parsing of translation keys
        :param key: Key to translate
        :return: Translated key for JSON
        """
        key = key.replace("ob_", "(").replace("_cb", ")")
        components = key.split("_")
        return " ".join(x.title() for x in components)

    def to_json(self) -> Dict[str, Any]:
        """
        Custom JSON serialization with key parsing
        :return: JSON object with parsed keys
        """
        return {
            self.parse_key(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
