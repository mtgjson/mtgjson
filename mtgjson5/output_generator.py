"""
MTGJSON output generator to write out contents to file & accessory methods
"""
import logging
import pathlib
from typing import Any, Dict, List

import simplejson as json

from .classes import MtgjsonDeckHeaderObject, MtgjsonMetaObject, MtgjsonSetObject
from .compiled_classes import (
    MtgjsonAllCardsObject,
    MtgjsonAllPrintingsObject,
    MtgjsonCardTypesObject,
    MtgjsonCompiledListObject,
    MtgjsonDeckListObject,
    MtgjsonKeywordsObject,
    MtgjsonSetListObject,
    MtgjsonStructuresObject,
)
from .consts import OUTPUT_PATH, SUPPORTED_FORMAT_OUTPUTS, SUPPORTED_SET_TYPES
from .providers.github_decks_provider import GithubDecksProvider

LOGGER = logging.getLogger(__name__)


def write_set_file(mtgjson_set_object: MtgjsonSetObject, pretty_print: bool) -> None:
    """
    Write MTGJSON Set out to a file
    :param mtgjson_set_object: Set to write out
    :param pretty_print: Should it be pretty or minimized?
    """
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.joinpath(f"{mtgjson_set_object.code}.json").open("w") as file:
        json.dump(
            obj={"data": mtgjson_set_object, "meta": MtgjsonMetaObject()},
            fp=file,
            for_json=True,
            sort_keys=True,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
        )


def generate_compiled_prices_output(
    price_data: Dict[str, Dict[str, float]], pretty_print: bool
) -> None:
    """
    Dump AllPrices to a file
    :param price_data: Data to dump
    :param pretty_print: Pretty or minimal
    """
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_prices, price_data, pretty_print,
    )


def generate_compiled_output_files(
    price_data: Dict[str, Dict[str, float]], pretty_print: bool
) -> None:
    """
    Create and dump all compiled outputs
    :param price_data: Price data to output
    :param pretty_print: Pretty or minimal
    """
    # AllPrices.json
    generate_compiled_prices_output(price_data, pretty_print)

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

    # Meta.json (Formerly version.json)
    log_and_create_compiled_output(
        MtgjsonStructuresObject().version, MtgjsonMetaObject(), pretty_print,
    )

    # SetList.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().set_list, MtgjsonSetListObject(), pretty_print
    )

    # AllPrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings,
        MtgjsonAllPrintingsObject(),
        pretty_print,
    )

    # Format specific set code split up
    format_map = construct_format_map()

    # StandardPrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings_standard,
        MtgjsonAllPrintingsObject(format_map["standard"]),
        pretty_print,
    )

    # PioneerPrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings_pioneer,
        MtgjsonAllPrintingsObject(format_map["pioneer"]),
        pretty_print,
    )

    # ModernPrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings_modern,
        MtgjsonAllPrintingsObject(format_map["modern"]),
        pretty_print,
    )

    # LegacyPrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings_legacy,
        MtgjsonAllPrintingsObject(format_map["legacy"]),
        pretty_print,
    )

    # VintagePrintings.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_printings_vintage,
        MtgjsonAllPrintingsObject(format_map["vintage"]),
        pretty_print,
    )

    # AllCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards, MtgjsonAllCardsObject(), pretty_print,
    )

    # Format specific card split up
    card_format_map = construct_all_cards_format_map()

    # StandardCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_standard,
        MtgjsonAllCardsObject(card_format_map["standard"]),
        pretty_print,
    )

    # PioneerCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_pioneer,
        MtgjsonAllCardsObject(card_format_map["pioneer"]),
        pretty_print,
    )

    # ModernCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_modern,
        MtgjsonAllCardsObject(card_format_map["modern"]),
        pretty_print,
    )

    # LegacyCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_legacy,
        MtgjsonAllCardsObject(card_format_map["legacy"]),
        pretty_print,
    )

    # VintageCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_vintage,
        MtgjsonAllCardsObject(card_format_map["vintage"]),
        pretty_print,
    )

    # PauperCards.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().all_cards_pauper,
        MtgjsonAllCardsObject(card_format_map["pauper"]),
        pretty_print,
    )

    # All Pre-constructed Decks
    deck_names = []
    for mtgjson_deck_obj in GithubDecksProvider().iterate_precon_decks():
        mtgjson_deck_header_obj = MtgjsonDeckHeaderObject(mtgjson_deck_obj)
        log_and_create_compiled_output(
            f"decks/{mtgjson_deck_header_obj.file_name}_{mtgjson_deck_header_obj.code}",
            mtgjson_deck_obj,
            pretty_print,
        )
        deck_names.append(mtgjson_deck_header_obj)

    # DeckLists.json
    log_and_create_compiled_output(
        MtgjsonStructuresObject().deck_lists,
        MtgjsonDeckListObject(deck_names),
        pretty_print,
    )


def log_and_create_compiled_output(
    compiled_name: str, compiled_object: Any, pretty_print: bool
) -> None:
    """
    Log and write out a compiled output file
    :param compiled_name: What file to save
    :param compiled_object: What content to write
    :param pretty_print: Pretty or minimal
    """
    LOGGER.info(f"Generating {compiled_name}")
    write_compiled_output_to_file(compiled_name, compiled_object, pretty_print)
    LOGGER.debug(f"Finished Generating {compiled_name}")


def write_compiled_output_to_file(
    file_name: str, file_contents: Any, pretty_print: bool
) -> None:
    """
    Dump content to a file in the outputs directory
    :param file_name: File to dump to
    :param file_contents: Contents to dump
    :param pretty_print: Pretty or minimal
    """
    write_file = OUTPUT_PATH.joinpath(f"{file_name}.json")
    write_file.parent.mkdir(parents=True, exist_ok=True)

    with write_file.open("w", encoding="utf-8") as file:
        json.dump(
            obj={"data": file_contents, "meta": MtgjsonMetaObject()},
            fp=file,
            for_json=True,
            sort_keys=True,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
        )


def construct_format_map(
    all_printings_path: pathlib.Path = OUTPUT_PATH.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    ),
    normal_sets_only: bool = True,
) -> Dict[str, List[str]]:
    """
    For each set in AllPrintings, determine what format(s) the set is
    legal in and put the set's key into that specific entry in the
    return value.
    :param all_printings_path: Path to AllPrintings.json
    :param normal_sets_only: Should we only handle normal sets
    :return: Format Map for future identifications
    """
    format_map: Dict[str, List[str]] = {
        magic_format: [] for magic_format in SUPPORTED_FORMAT_OUTPUTS
    }

    if not all_printings_path.is_file():
        LOGGER.warning(f"{all_printings_path} was not found, skipping format map")
        return {}

    with all_printings_path.open(encoding="utf-8") as file:
        content = json.load(file)

    for set_code_key, set_code_content in content.get("data", {}).items():
        if normal_sets_only and set_code_content.get("type") not in SUPPORTED_SET_TYPES:
            continue

        formats_set_legal_in = SUPPORTED_FORMAT_OUTPUTS
        for card in set_code_content.get("cards"):
            card_legalities = set(card.get("legalities").keys())
            formats_set_legal_in = formats_set_legal_in.intersection(card_legalities)

        for magic_format in formats_set_legal_in:
            format_map[magic_format].append(set_code_key)

    return format_map


def construct_all_cards_format_map(
    all_printings_path: pathlib.Path = OUTPUT_PATH.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    ),
) -> Dict[str, Any]:
    """
    Construct a format map for cards instead of sets,
    allowing for easy parsing and dispatching to different
    files.
    :param all_printings_path: Path to AllPrintings.json
    :return: Cards in a format map
    """
    format_card_map: Dict[str, Dict[str, Dict[str, Any]]] = {
        magic_format: {} for magic_format in SUPPORTED_FORMAT_OUTPUTS
    }

    if not all_printings_path.is_file():
        LOGGER.warning(f"{all_printings_path} was not found, skipping format map")
        return {}

    with all_printings_path.open(encoding="utf-8") as file:
        content = json.load(file)

    for set_contents in content.get("data", {}).values():
        for card in set_contents.get("cards"):
            for magic_format in format_card_map.keys():
                if card.get("legalities").get(magic_format) in {"Legal", "Restricted"}:
                    format_card_map[magic_format][card["name"]] = card

    return format_card_map
