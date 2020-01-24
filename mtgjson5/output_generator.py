"""
MTGJSON output generator to write out contents to file & accessory methods
"""
from typing import Any

from .classes import (
    MtgjsonSetObject,
    MtgjsonMetaObject,
)
from .compiled_classes import (
    MtgjsonCardTypesObject,
    MtgjsonCompiledListObject,
    MtgjsonKeywordsObject,
    MtgjsonStructuresObject,
    MtgjsonSetListObject,
)
from .consts import OUTPUT_PATH
from .utils import get_thread_logger
import simplejson as json

LOGGER = get_thread_logger()


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


def generate_compiled_output_files(pretty_print: bool) -> None:
    """
    Create and dump all compiled outputs
    :param pretty_print: Pretty or minimal
    """
    # CompiledList.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().compiled_list,
        MtgjsonCompiledListObject(),
        pretty_print,
    )

    # Keywords.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().key_words, MtgjsonKeywordsObject(), pretty_print,
    )

    # CardTypes.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().card_types, MtgjsonCardTypesObject(), pretty_print,
    )

    # version.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().version, MtgjsonMetaObject(), pretty_print,
    )

    # SetList.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().set_list, MtgjsonSetListObject(), pretty_print
    )


def log_and_create_compiled_output(
    compiled_name, compiled_object, pretty_print
) -> None:
    """
    Log and write out a compiled output file
    :param compiled_name: What file to save
    :param compiled_object: What content to write
    :param pretty_print: Pretty or minimal
    """
    LOGGER.info(f"Generating {compiled_name}")
    write_compiled_output_to_file(compiled_name, compiled_object, pretty_print)


def write_compiled_output_to_file(
    file_name: str, file_contents: Any, pretty_print: bool
) -> None:
    """
    Dump content to a file in the outputs directory
    :param file_name: File to dump to
    :param file_contents: Contents to dump
    :param pretty_print: Pretty or minimal
    """
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.joinpath(f"{file_name}.json").open("w", encoding="utf-8") as file:
        json.dump(
            obj=file_contents,
            fp=file,
            for_json=True,
            sort_keys=True,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
        )
