"""
MTGJSON SetList Object
"""

import json
from typing import Any

from ..classes.json_object import JsonObject
from ..mtgjson_config import MtgjsonConfig
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonSetListObject(JsonObject):
    """
    MTGJSON SetList Object
    """

    set_list: list[dict[str, str]]

    def __init__(self) -> None:
        """
        Initializer to build up the object
        """
        self.set_list = self.get_all_set_list(files_to_ignore=MtgjsonStructuresObject().get_all_compiled_file_names())

    @staticmethod
    def get_all_set_list(files_to_ignore: list[str]) -> list[dict[str, str]]:
        """
        This will create the SetList.json file
        by getting the info from all the files in
        the set_outputs folder and combining
        them into the old v3 structure.
        :param files_to_ignore: Files to ignore in set_outputs folder
        :return: List of all set dicts
        """
        all_sets_data: list[dict[str, str]] = []

        for set_file in MtgjsonConfig().output_path.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as f:
                set_data = json.load(f).get("data", {})

            if not set_data.get("name"):
                continue

            for key in ["booster", "cards", "tokens"]:
                if key in set_data:
                    del set_data[key]

            all_sets_data.append(set_data)

        return sorted(all_sets_data, key=lambda set_info: set_info["name"])

    def to_json(self) -> list[Any]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.set_list
