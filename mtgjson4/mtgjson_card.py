from typing import Any, Dict, Iterator, KeysView, Tuple
import uuid

from mtgjson4.provider import scryfall


class MTGJSONCard:
    def __init__(self, set_code: str) -> None:
        self.card_attributes: Dict[str, Any] = {}
        self.set_code = set_code.upper()

    def set_attribute(self, attribute_name: str, attribute_value: Any) -> None:
        self.get_attributes()[attribute_name] = attribute_value

    def get_attribute(self, attribute_name: str, default_value: Any = None) -> Any:
        if attribute_name in self.get_attributes():
            return self.get_attributes()[attribute_name]
        return default_value

    def get_attributes(self) -> Dict[str, Any]:
        return self.card_attributes

    def get_uuid(self) -> str:
        """
        Get unique card face identifier.
        :return: unique card face identifier
        """
        #  As long as all cards have scryfallId (scryfallId, name) is enough to uniquely identify the card face
        # PROVIDER_ID prevents collision with card IDs from any future card provider
        id_source = (
            scryfall.PROVIDER_ID
            + self.get_attribute("scryfallId")
            + self.get_attribute("name")
        )
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source))

    def get_uuid_421(self) -> str:
        """
        Get card uuid used in MTGJSON release 4.2.1
        :return: unique card face identifier
        """
        # Use attributes that _shouldn't_ change over time
        # Name + set code + colors (if applicable) + Scryfall UUID + printed text (if applicable)
        id_source = (
            self.get_attribute("name")
            + self.set_code
            + "".join(self.get_attribute("colors", ""))
            + self.get_attribute("scryfallId")
            + str(self.get_attribute("originalText", ""))
        )
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source))

    def keys(self) -> KeysView:
        return self.get_attributes().keys()

    def how_many_names(self, how_many_expected: int = 0) -> bool:
        return how_many_expected == len(self.get_attribute("names", []))

    def append_attribute(self, attribute_name: str, attribute_value: Any) -> None:
        if attribute_name in self.keys():
            self.get_attributes()[attribute_name].append(attribute_value)
        else:
            self.set_attribute(attribute_name, attribute_value)

    def remove_attribute(self, attribute_name: str) -> bool:
        if attribute_name in self.keys():
            del self.get_attributes()[attribute_name]
            return True
        return False

    def items(self) -> Iterator[Tuple[str, Any]]:
        for key in self.keys():
            yield key, self.get_attribute(key)
