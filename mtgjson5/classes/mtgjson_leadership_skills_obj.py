"""
MTGJSON container for Leadership Skills
"""
import json
from typing import Any, Dict

from mtgjson5.globals import to_camel_case


class MtgjsonLeadershipSkillsObject(json.JSONEncoder):
    """
    Container for Leadership Skills
    """

    brawl: bool
    commander: bool
    oathbreaker: bool

    def __init__(self, brawl, commander, oathbreaker):
        self.brawl = brawl
        self.commander = commander
        self.oathbreaker = oathbreaker

    def for_json(self) -> Dict[str, Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        }
