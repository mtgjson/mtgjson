"""
MTGJSON Card Class Container
"""
import contextvars
import json
import logging
from typing import Any, Callable, Dict, Iterator, KeysView, List, Optional, Tuple
import uuid

import mtgjson4
from mtgjson4.provider import tcgplayer

TCGPLAYER_REFERRAL: str = "?partner=mtgjson&utm_campaign=affiliate&utm_medium=mtgjson&utm_source=mtgjson"
DUEL_DECK_LAND_MARKED: contextvars.ContextVar = contextvars.ContextVar("DD_R1")
DUEL_DECK_SIDE_COMP: contextvars.ContextVar = contextvars.ContextVar("DD_R2")

LOGGER = logging.getLogger(__name__)


class MTGJSONCard:
    """
    MTGJSON Card Class
    """

    def __init__(self, set_code: str) -> None:
        """
        Initializer
        :param set_code: Set Code this card is found in
        """
        self.card_attributes: Dict[str, Any] = {}
        self.set_code: str = set_code.upper()
        self.tcgplayer_url: str = ""

    def __str__(self) -> str:
        """
        String representation of class
        :return: String of attributes
        """
        return str(self.card_attributes)

    def clear(self) -> None:
        """
        Clear all attributes on the card
        :return:
        """
        self.card_attributes.clear()

    def get(self, attribute_name: str, default_value: Any = None) -> Any:
        """
        Given an attribute, return value if found in internal dictionary
        :param attribute_name: Key
        :param default_value: Value if key not in dict
        :return: Value or default_value
        """
        if attribute_name in self.get_all():
            return self.get_all()[attribute_name]
        return default_value

    def get_all(self) -> Dict[str, Any]:
        """
        Return internal dictionary
        :return: Internal dictionary
        """
        return self.card_attributes

    def set(
        self, attribute_name: str, attribute_value: Any, value_function: Callable = None
    ) -> None:
        """
        Given an attribute, add it to our internal dictionary
        :param attribute_name: Key
        :param attribute_value: Value
        :param value_function: Function to run on value before inserting
        """
        if value_function:
            attribute_value = value_function(attribute_value)

        self.get_all()[attribute_name] = attribute_value

    def set_all(self, attribute_dict: Dict[str, Any]) -> None:
        """
        Given a dict of attributes, add them to ours
        :param attribute_dict: Dict of attributes
        """
        for key, value in attribute_dict.items():
            self.set(key, value)

    def get_tcgplayer_url(self) -> str:
        """
        Get TCGPlayer with affiliate code
        :return:
        """
        return str(self.tcgplayer_url) + TCGPLAYER_REFERRAL

    def keys(self) -> KeysView:
        """
        Return internal dictionary keys
        :return: Keys
        """
        return self.get_all().keys()

    def names_count(self, how_many_expected: int = 0) -> bool:
        """
        Check if there are a certain number of names to a card
        :param how_many_expected: How many use expects
        :return: Match requirements of user input
        """
        return how_many_expected == len(self.get("names", []))

    def append(self, attribute_name: str, attribute_value: Any) -> None:
        """
        If key exists, append value to old value. Otherwise, add to internal dict
        :param attribute_name: Key
        :param attribute_value: Value
        """
        if attribute_name in self.keys():
            if isinstance(self.get_all()[attribute_name], list):
                self.get_all()[attribute_name].append(attribute_value)
            else:
                self.get_all()[attribute_name] += attribute_value
        else:
            self.set(attribute_name, attribute_value)

    def remove(self, attribute_name: str) -> bool:
        """
        Delete an attribute from internal dict
        :param attribute_name: Key
        :return: Deleted successfully
        """
        if attribute_name in self.keys():
            del self.get_all()[attribute_name]
            return True
        return False

    def items(self) -> Iterator[Tuple[str, Any]]:
        """
        Reimplementation of dict.items()
        :return: Iterator of item pairs
        """
        for key in self.keys():
            yield key, self.get(key)

    def add_tcgplayer_fields(self, tcg_card_objs: List[Dict[str, Any]]) -> None:
        """
        Add the tcgplayer fields to the internal dict
        :param tcg_card_objs: Attributes to handle
        """
        if not self.get("tcgplayerProductId"):
            self.set(
                "tcgplayerProductId",
                tcgplayer.get_card_property(
                    self.get("name"), tcg_card_objs, "productId"
                ),
            )

        prod_url = tcgplayer.get_card_property(self.get("name"), tcg_card_objs, "url")

        if self.get("tcgplayerProductId") and prod_url:
            self.set(
                "tcgplayerPurchaseUrl",
                tcgplayer.log_redirection_url(self.get("tcgplayerProductId")),
            )

        self.tcgplayer_url = tcgplayer.get_card_property(
            self.get("name"), tcg_card_objs, "url"
        )

    def clean_up_watermark(self, watermark: Optional[str]) -> Optional[str]:
        """
        Scryfall (currently) doesn't provide what set watermarks
        are of, only "set" so we will add it ourselves using
        a resources file MTGJSON generated offline
        :param watermark: Current watermark
        :return optional value
        """
        if not watermark:
            return None

        if watermark == "set":
            with mtgjson4.RESOURCE_PATH.joinpath("set_code_watermarks.json").open(
                "r", encoding="utf-8"
            ) as f:
                json_dict: Dict[str, List[Any]] = json.load(f)

                for card in json_dict[self.set_code]:
                    if self.get("name") in card["name"].split(" // "):
                        return str(card["watermark"])

        return watermark

    def final_card_cleanup(self, is_card: bool = True) -> None:
        """
        Re-add and cleanup content from cards that are dependent
        on other values
        :param is_card: Card or token
        """
        self.set("uuid", self.get_uuid(is_card))
        self.set("uuidV421", self.get_uuid_421())

        if self.set_code.startswith("DD"):
            self.__mark_duel_decks()

        self.__remove_unnecessary_fields()

    def get_uuid(self, is_card: bool = True) -> str:
        """
        Get unique card face identifier.
        :return: unique card face identifier
        """
        if is_card:
            #  As long as all cards have scryfallId (scryfallId, name) is enough to uniquely identify the card face
            # PROVIDER_ID prevents collision with card IDs from any future card provider
            id_source = (
                mtgjson4.SCRYFALL_PROVIDER_ID
                + self.get("scryfallId")
                + self.get("name")
            )
        else:
            id_source = (
                self.get("name")
                + "".join(self.get("colors", ""))
                + str(self.get("power", ""))
                + str(self.get("toughness", ""))
                + str(self.get("side", ""))
                + self.set_code[1:]  # Token sets start with a "T"
                + self.get("scryfallId")
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
            self.get("name")
            + self.set_code
            + "".join(self.get("colors", ""))
            + self.get("scryfallId")
            + str(self.get("originalText", ""))
        )
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, id_source))

    # -----------------------
    # Private functions below
    # -----------------------
    def __mark_duel_decks(self) -> None:
        """
        Duel decks are usually put together where the cards
        in the first deck are at the beginning, followed
        by basics, then start the second deck. We exploit
        this property to mark them as decks "a" and "b"
        """
        if self.get("name") in mtgjson4.BASIC_LANDS:
            DUEL_DECK_LAND_MARKED.set(True)
        elif DUEL_DECK_LAND_MARKED.get():
            DUEL_DECK_SIDE_COMP.set(chr(ord(DUEL_DECK_SIDE_COMP.get()) + 1))
            DUEL_DECK_LAND_MARKED.set(False)

        self.set("duelDeck", DUEL_DECK_SIDE_COMP.get())

    def __remove_unnecessary_fields(self) -> None:
        """
        Remove invalid field entries to shrink JSON output size
        """
        remove_field_if_false: List[str] = [
            "isOversized",
            "isOnlineOnly",
            "isTimeshifted",
            "isReserved",
            "frameEffect",
        ]

        insert_value = {}

        for key, value in self.items():
            if value is not None:
                if (key in remove_field_if_false and value is False) or (value == ""):
                    continue
                if key == "foreignData":
                    value = self.__fix_foreign_entries(value)
                elif key == "names":
                    if len(value) != len(set(value)):
                        continue
                insert_value[key] = value

        self.clear()
        self.set_all(insert_value)

    @staticmethod
    def __fix_foreign_entries(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Foreign entries may have bad values, such as missing flavor text. This removes them.
        :param values: List of foreign entries dicts
        :return: Pruned foreign entries
        """
        # List of dicts
        fd_insert_list = []
        for foreign_info in values:
            fd_insert_dict = {}

            name_found: bool = False
            for fd_key, fd_value in foreign_info.items():
                if fd_value is not None:
                    fd_insert_dict[fd_key] = fd_value

                    if fd_key == "name":
                        name_found = True

            if name_found:
                fd_insert_list.append(fd_insert_dict)

        return fd_insert_list
