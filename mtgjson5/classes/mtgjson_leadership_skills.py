"""
MTGJSON Singular Card.LeadershipSkills Object
"""

from .json_object import JsonObject


class MtgjsonLeadershipSkillsObject(JsonObject):
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
