"""
MTGJSON AllPrintings Object
"""
import json
import pathlib
from typing import Dict, List

from ..classes import MtgjsonSetObject
from ..consts import OUTPUT_PATH
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonAllPrintingsObject:
    """
    MTGJSON AllPrintings Object
    """

    all_sets_dict: Dict[str, MtgjsonSetObject]

    def __init__(self) -> None:
        """
        Initialize to build up the object
        """
        self.all_sets_dict = {}
        files_to_build = self.get_files_to_build(
            MtgjsonStructuresObject().get_all_compiled_file_names()
        )
        self.iterate_all_sets(files_to_build)

    def get_set_contents(self, sets: List[str] = None) -> Dict[str, MtgjsonSetObject]:
        """
        Give the contents of certain sets. Empty for all sets.
        :param sets: Sets to get. Empty for all sets.
        :return Subset of AllPrintings sets
        """
        if sets:
            return {
                key: self.all_sets_dict[key]
                for key in sets
                if key in self.all_sets_dict
            }

        return self.all_sets_dict

    @staticmethod
    def get_files_to_build(files_to_ignore: List[str]) -> List[pathlib.Path]:
        """
        Determine what file(s) to include in the build
        :param files_to_ignore: Files to exclude
        :return: Files
        """
        return [
            file_path
            for file_path in OUTPUT_PATH.glob("*.json")
            if file_path.stem not in files_to_ignore
        ]

    def iterate_all_sets(self, files_to_build: List[pathlib.Path]) -> None:
        """
        Iterate and all all MTGJSON sets to the dictionary
        indexed by file name
        :param files_to_build: Files to include
        """
        for set_file in files_to_build:
            with set_file.open(encoding="utf-8") as file:
                file_content = json.load(file)

            # Account for the CON fix
            set_code = set_file.stem
            if set_code.endswith("_"):
                set_code = set_code[:-1]

            self.all_sets_dict[set_code] = file_content.get("data", {})

    def to_json(self) -> Dict[str, MtgjsonSetObject]:
        """
        Support json.dump()
        :return: JSON serialized object
        """
        return self.all_sets_dict
