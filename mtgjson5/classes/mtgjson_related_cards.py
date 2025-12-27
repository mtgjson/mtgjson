"""
MTGJSON Related Cards Container
"""


from ..classes.json_object import JsonObject


class MtgjsonRelatedCardsObject(JsonObject):
    """
    MTGJSON Related Cards Container
    """

    reverse_related: list[str]
    spellbook: list[str]

    def __init__(self) -> None:
        self.reverse_related = []
        self.spellbook = []

    def present(self) -> bool:
        """
        Determine if this object contains any values
        :return Object contains values
        """
        return bool(self.reverse_related or self.spellbook)

    def build_keys_to_skip(self) -> list[str]:
        keys_to_skip = []
        if not self.reverse_related:
            keys_to_skip.append("reverse_related")
        if not self.spellbook:
            keys_to_skip.append("spellbook")

        return keys_to_skip
