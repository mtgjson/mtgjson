"""MTGJSON Leadership Skills Object model for commander eligibility."""

from ..mtgjson_base import MTGJsonModel


class MtgjsonLeadershipSkillsObject(MTGJsonModel):
    """
    MTGJSON Singular Card.LeadershipSkills Object
    """

    brawl: bool
    commander: bool
    oathbreaker: bool
