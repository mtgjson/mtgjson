from ..mtgjson_base import MTGJsonModel


class MtgjsonLeadershipSkillsObject(MTGJsonModel):
    """
    MTGJSON Singular Card.LeadershipSkills Object
    """

    brawl: bool
    commander: bool
    oathbreaker: bool

    def __init__(self, brawl: bool, commander: bool, oathbreaker: bool, **data):
        """
        Initialize leadership skills
        :param brawl: Brawl legal
        :param commander: Commander legal
        :param oathbreaker: Oathbreaker legal
        """
        super().__init__(brawl=brawl, commander=commander, oathbreaker=oathbreaker, **data)
