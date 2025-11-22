"""MTGJSON Related Cards Object model for card relationships and references."""

from typing import List, Set

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonRelatedCardsObject(MTGJsonModel):
    """
    MTGJSON Related Cards Container
    """

    reverse_related: List[str] = Field(default_factory=list)
    spellbook: List[str] = Field(default_factory=list)

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
