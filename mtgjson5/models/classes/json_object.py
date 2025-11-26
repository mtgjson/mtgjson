"""MTGJSON JSON Object wrapper for top-level data structures."""

from typing import Any, Dict, Set

from ..mtgjson_base import MTGJsonModel


class JsonObject(MTGJsonModel):
    """
    Top level Json Dump object class
    """

    def build_keys_to_skip(self) -> Set[str]:
        """
        Determine what keys should be avoided in the JSON dump
        :return: Keys to avoid
        """
        return set()

    def to_json(self) -> Dict[str, Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        skip_keys = self.build_keys_to_skip()

        # Use Pydantic's model_dump with camelCase aliases
        result = self.model_dump(by_alias=True, exclude_none=True, mode="json")

        # Filter out skipped keys
        for key in skip_keys:
            result.pop(key, None)

        return result
