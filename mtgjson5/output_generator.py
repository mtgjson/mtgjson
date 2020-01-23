"""
MTGJSON output generator to write out contents to file & accessory methods
"""
from typing import Any, Dict

from mtgjson5.classes import MtgjsonSetObject
from mtgjson5.classes.mtgjson_structures_obj import MtgjsonStructuresObject
from mtgjson5.consts import (
    MTGJSON_BUILD_DATE,
    MTGJSON_PRICE_BUILD_DATE,
    MTGJSON_VERSION,
    OUTPUT_PATH,
)
from mtgjson5.providers import WizardsProvider
import simplejson as json

from mtgjson5.utils import get_thread_logger

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


def get_meta_information() -> Dict[str, str]:
    """
    Get MTGJSON meta information
    :return: Meta information
    """
    return {
        "version": MTGJSON_VERSION,
        "date": MTGJSON_BUILD_DATE,
        "pricesDate": MTGJSON_PRICE_BUILD_DATE,
    }


def create_compiled_list_output() -> Dict[str, Any]:
    """
    Create the compiled list output file
    :return: CompiledList.json file content
    """
    return {
        "files": sorted(MtgjsonStructuresObject().get_compiled_list_files()),
        "meta": get_meta_information(),
    }


def create_keywords_output() -> Dict[str, Any]:
    """
    Give a compiled dictionary result of the key phrases that can be
    found in the MTG comprehensive rule book.
    :return: Keywords.json file content
    """
    return {
        "abilityWords": WizardsProvider().get_magic_ability_words(),
        "keywordActions": WizardsProvider().get_keyword_actions(),
        "keywordAbilities": WizardsProvider().get_keyword_abilities(),
        "meta": get_meta_information(),
    }


def create_card_types_output() -> Dict[str, Any]:
    """
    Create the card types list output file
    :return: CardTypes.json file c ontent
    """
    return {"types": WizardsProvider().get_card_types(), "meta": get_meta_information()}


def generate_compiled_output_files(pretty_print: bool) -> None:
    """
    Create and dump all compiled outputs
    :param pretty_print: Pretty or minimal
    """
    # CompiledList.json
    LOGGER.info(f"Generating {MtgjsonStructuresObject().compiled_list}")
    write_compiled_output_to_file(
        MtgjsonStructuresObject().compiled_list,
        create_compiled_list_output(),
        pretty_print,
    )

    # Keywords.json
    LOGGER.info(f"Generating {MtgjsonStructuresObject().key_words}")
    write_compiled_output_to_file(
        MtgjsonStructuresObject().key_words, create_keywords_output(), pretty_print
    )

    # CardTypes.json
    LOGGER.info(f"Generating {MtgjsonStructuresObject().card_types}")
    write_compiled_output_to_file(
        MtgjsonStructuresObject().card_types, create_card_types_output(), pretty_print
    )


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
            sort_keys=True,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
        )
