"""
MTGJSON AllPrintings container
"""
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

    def __init__(self) -> None:
        self.all_sets_dict = {}
        self.iterate_all_sets(MtgjsonStructuresObject().get_all_compiled_file_names())

    def iterate_all_sets(self, files_to_ignore: List[str]) -> None:
        """
        Iterate and all all MTGJSON sets to the dictionary
        indexed by file name
        :param files_to_ignore: Files to skip
        """
        for set_file in OUTPUT_PATH.glob("*.json"):
            if set_file.stem in files_to_ignore:
                continue

            with set_file.open(encoding="utf-8") as file:
                file_content = json.load(file)

            self.all_sets_dict[set_file.stem] = file_content

    def for_json(self) -> Dict[str, MtgjsonSetObject]:
        """
        Support json.dumps()
        :return: JSON serialized object
        """
        return self.all_sets_dict
