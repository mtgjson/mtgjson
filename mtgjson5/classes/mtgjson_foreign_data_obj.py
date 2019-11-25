"""
MTGJSON container for foreign entries
"""
from typing import Dict, Any


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

    def __init__(self):
        pass

    def default(self, o):
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
