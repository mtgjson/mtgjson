"""MTGJSON Rulings Object model for official card rulings."""

from ..mtgjson_base import MTGJsonModel


class MtgjsonRulingObject(MTGJsonModel):
    """
    MTGJSON Singular Card.Rulings Object
    """

    date: str
    text: str
