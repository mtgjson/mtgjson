"""
MTGJSON AllPrintings container
"""
import pathlib
from typing import Dict, List

import simplejson as json

from ..classes import MtgjsonSetObject
from ..consts import OUTPUT_PATH
from .mtgjson_structures_obj import MtgjsonStructuresObject


class MtgjsonAllPrintingsObject:
    """
    AllPrintings container
    """

    all_sets_dict: Dict[str, MtgjsonSetObject]

    def __init__(self, whitelist_sets: List[str] = None) -> None:
        self.all_sets_dict = {}

        files_to_build = self.get_files_to_build(
            MtgjsonStructuresObject().get_all_compiled_file_names(), whitelist_sets
        )
        self.iterate_all_sets(files_to_build)

    @staticmethod
    def get_files_to_build(
        files_to_ignore: List[str], whitelist_files: List[str] = None,
    ) -> List[pathlib.Path]:
        """
        Determine what file(s) to include in the build
        :param files_to_ignore: Files to exclude
        :param whitelist_files: Files to include
        :return: Files
        """
        if whitelist_files:
            return [
                file_path
                for file_path in OUTPUT_PATH.glob("*.json")
                if file_path.stem in whitelist_files
                and file_path.stem not in files_to_ignore
            ]

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

            self.all_sets_dict[set_file.stem] = file_content

    def for_json(self) -> Dict[str, MtgjsonSetObject]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return self.all_sets_dict
