"""
MTGJSON Singular Deck Object
"""
import re
from typing import Any, Dict, List

from ..utils import to_camel_case


class MtgjsonDeckObject:
    """
    MTGJSON Singular Card Object
    """

    code: str
    commander: List[Dict[str, Any]]  # MtgjsonCardObject
    main_board: List[Dict[str, Any]]  # MtgjsonCardObject
    name: str
    side_board: List[Dict[str, Any]]  # MtgjsonCardObject
    release_date: str
    type: str
    file_name: str

    def set_sanitized_name(self, name: str) -> None:
        """
        Turn an unsanitary file name to a safe one
        :param name: Unsafe name
        """
        word_characters_only_regex = re.compile(r"[^\w]")
        capital_case = "".join(x for x in name.title() if not x.isspace())

        deck_name_sanitized = word_characters_only_regex.sub("", capital_case)

        self.file_name = f"{deck_name_sanitized}_{self.code}"

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        skip_keys = {"file_name"}

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value) and key not in skip_keys
        }
