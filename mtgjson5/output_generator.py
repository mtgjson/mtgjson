"""
MTGJSON output generator to write out contents to file & accessory methods
"""
from mtgjson5.classes import MtgjsonSetObject
from mtgjson5.consts import OUTPUT_PATH
import simplejson as json


def write_set_file(mtgjson_set_object: MtgjsonSetObject, pretty_print: bool) -> None:
    """
    Write MTGJSON Set out to a file
    :param mtgjson_set_object: Set to write out
    :param pretty_print: Should it be pretty or minimized?
    """
    with OUTPUT_PATH.joinpath(f"{mtgjson_set_object.code}.json").open("w") as file:
        json.dump(
            obj=mtgjson_set_object,
            fp=file,
            for_json=True,
            sort_keys=True,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
        )
