from typing import List
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

    def build_keys_to_skip(self) -> List[str]:
        """
        Build keys to skip in JSON output
        :return: List of keys to skip
        """
        keys_to_skip = []
        if not self.reverse_related:
            keys_to_skip.append("reverse_related")
        if not self.spellbook:
            keys_to_skip.append("spellbook")
        return keys_to_skip
