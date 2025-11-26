"""MTGJSON Related Cards Object model for card relationships and references."""

from typing import List, Set

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonRelatedCardsObject(MTGJsonModel):
    """
    The Related Cards Data Model describes the properties of a card that has relations to other cards.
    """

    reverse_related: List[str] = Field(
        default_factory=list,
        description="A list of card names associated to a card, such as 'meld' cards and token creation.",
    )
    spellbook: List[str] = Field(
        default_factory=list,
        description="A list of card names associated to a card's Spellbook mechanic.",
    )

    def present(self) -> bool:
        """
        Determine if this object contains any values
        :return: Object contains values
        """
        return bool(self.reverse_related or self.spellbook)

    def build_keys_to_skip(self) -> Set[str]:
        """
        Build keys to skip in JSON output
        :return: Set of keys to skip
        """
        keys_to_skip: Set[str] = set()
        if not self.reverse_related:
            keys_to_skip.add("reverse_related")
        if not self.spellbook:
            keys_to_skip.add("spellbook")
        return keys_to_skip
