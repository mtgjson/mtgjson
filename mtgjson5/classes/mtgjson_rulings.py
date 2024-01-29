"""
MTGJSON Singular Card.Rulings Object
"""
from .json_object import JsonObject


class MtgjsonRulingObject(JsonObject):
    """
    MTGJSON Singular Card.Rulings Object
    """

    date: str
    text: str

    def __init__(self, date: str, text: str) -> None:
        """
        Set the ruling date and text
        """
        self.date = date
        self.text = text
