"""
MTGJSON container for Leadership Skills
"""
import json
from typing import Dict, Any


class MtgjsonLeadershipSkillsObject(json.JSONEncoder):
    """
    Container for Leadership Skills
    """

    brawl: bool
    commander: bool
    oathbreaker: bool

    def __init__(self):
        pass

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
