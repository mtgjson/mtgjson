"""
MTGJSON DeckList Object
"""


from ..classes import MtgjsonDeckHeaderObject
from ..classes.json_object import JsonObject


class MtgjsonDeckListObject(JsonObject):
    """
    MTGJSON DeckList Object
    """

    decks: list[MtgjsonDeckHeaderObject]

    def __init__(self, deck_headers: list[MtgjsonDeckHeaderObject]) -> None:
        """
        Initializer to build up the object
        """
        self.decks = deck_headers

    def to_json(self) -> list[MtgjsonDeckHeaderObject]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.decks
