"""
MTGJSON Singular Card.LeadershipSkills Object
"""
from typing import Any, Dict

from ..utils import to_camel_case


class MtgjsonLeadershipSkillsObject:
    """
    MTGJSON Singular Card.LeadershipSkills Object
    """

    brawl: bool
    commander: bool
    oathbreaker: bool

    def __init__(self, brawl: bool, commander: bool, oathbreaker: bool) -> None:
        self.brawl = brawl
        self.commander = commander
        self.oathbreaker = oathbreaker

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return {
            to_camel_case(key): value
            for key, value in self.__dict__.items()
            if "__" not in key and not callable(value)
        }
