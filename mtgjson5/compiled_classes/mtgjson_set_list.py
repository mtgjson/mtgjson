"""
MTGJSON SetList container
"""
from typing import Any, Dict, List

import simplejson as json

from ..consts import OUTPUT_PATH
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonSetListObject:
    """
    SetList container
    """

    set_list: List[Dict[str, str]]

    def __init__(self) -> None:
        self.set_list = self.get_all_set_list(
            MtgjsonStructuresObject().get_all_compiled_file_names()
        )

    @staticmethod
    def get_all_set_list(files_to_ignore: List[str]) -> List[Dict[str, str]]:
        """
        This will create the SetList.json file
        by getting the info from all the files in
        the set_outputs folder and combining
        them into the old v3 structure.
        :param files_to_ignore: Files to ignore in set_outputs folder
        :return: List of all set dicts
        """
        all_sets_data: List[Dict[str, str]] = []

        for set_file in OUTPUT_PATH.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as f:
                file_content = json.load(f)

            if not file_content.get("name"):
                continue

            set_data = {
                "baseSetSize": file_content.get("baseSetSize"),
                "code": file_content.get("code"),
                "meta": file_content.get("meta"),
                "name": file_content.get("name"),
                "releaseDate": file_content.get("releaseDate"),
                "totalSetSize": file_content.get("totalSetSize"),
                "type": file_content.get("type"),
            }

            if "parentCode" in file_content.keys():
                set_data["parentCode"] = file_content["parentCode"]

            all_sets_data.append(set_data)

        return sorted(all_sets_data, key=lambda set_info: set_info["name"])

    def for_json(self) -> List[Any]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return [
            value
            for key, value in self.__dict__.items()
            if not key.startswith("__") and not callable(value)
        ]
