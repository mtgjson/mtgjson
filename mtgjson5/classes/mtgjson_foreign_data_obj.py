"""
MTGJSON container for foreign entries
"""

from typing import Any, Dict

from mtgjson5.globals import to_camel_case


class MtgjsonForeignDataObject:
    """
    Foreign data rows
    """

    flavor_text: str
    language: str
    multiverse_id: int
    name: str
    text: str
    type: str

    url: str
    number: float
    set_code: str

    def __init__(self):
        pass

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        skip_keys = {"url", "number", "set_code"}

        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value) and key not in skip_keys
        }
