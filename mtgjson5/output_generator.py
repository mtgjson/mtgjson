"""
MTGJSON output generator to write out contents to file & accessory methods
"""
import json
import logging
import pathlib
from typing import Any, Dict, List

from . import constants
from .classes import MtgjsonDeckHeaderObject, MtgjsonMetaObject
from .compiled_classes import (
    MtgjsonAllIdentifiersObject,
    MtgjsonAllPrintingsObject,
    MtgjsonAtomicCardsObject,
    MtgjsonCardTypesObject,
    MtgjsonCompiledListObject,
    MtgjsonDeckListObject,
    MtgjsonEnumValuesObject,
    MtgjsonKeywordsObject,
    MtgjsonSetListObject,
    MtgjsonStructuresObject,
    MtgjsonTcgplayerSalesHistoryObject,
    MtgjsonTcgplayerSkusObject,
)
from .mtgjson_config import MtgjsonConfig
from .price_builder import build_prices, download_live_mtgjson_file
from .providers import GitHubDecksProvider
from .utils import get_file_hash

LOGGER = logging.getLogger(__name__)


def generate_compiled_prices_output(
    price_data: Dict[str, Dict[str, float]], pretty_print: bool
) -> None:
    """
    Dump AllPrices to a file
    :param price_data: Data to dump
    :param pretty_print: Pretty or minimal
    """
    LOGGER.info("Building Prices")
    create_compiled_output(
        MtgjsonStructuresObject().all_prices, price_data, pretty_print, sort_keys=False
    )


def build_format_specific_files(
    all_printings: MtgjsonAllPrintingsObject, pretty_print: bool
) -> None:
    """
    Compile *Printings files based on AllPrintings
    :param all_printings: Holder of AllPrintings content
    :param pretty_print: Should outputs be pretty or minimal
    """
    # Format specific set code split up
    format_map = construct_format_map()

    # Standard.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings_standard,
        all_printings.get_set_contents(format_map["standard"]),
        pretty_print,
    )

    # Pioneer.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings_pioneer,
        all_printings.get_set_contents(format_map["pioneer"]),
        pretty_print,
    )

    # Modern.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings_modern,
        all_printings.get_set_contents(format_map["modern"]),
        pretty_print,
    )

    # Legacy.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings_legacy,
        all_printings.get_set_contents(format_map["legacy"]),
        pretty_print,
    )

    # Vintage.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings_vintage,
        all_printings.get_set_contents(format_map["vintage"]),
        pretty_print,
    )


def build_atomic_specific_files(pretty_print: bool) -> None:
    """
    Compile *Atomic files based on AtomicCards
    :param pretty_print: Should outputs be pretty or minimal
    """
    # Format specific card split up
    card_format_map = construct_atomic_cards_format_map()

    # StandardCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_standard,
        MtgjsonAtomicCardsObject(card_format_map["standard"]),
        pretty_print,
    )

    # PioneerCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_pioneer,
        MtgjsonAtomicCardsObject(card_format_map["pioneer"]),
        pretty_print,
    )

    # ModernCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_modern,
        MtgjsonAtomicCardsObject(card_format_map["modern"]),
        pretty_print,
    )

    # LegacyCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_legacy,
        MtgjsonAtomicCardsObject(card_format_map["legacy"]),
        pretty_print,
    )

    # VintageCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_vintage,
        MtgjsonAtomicCardsObject(card_format_map["vintage"]),
        pretty_print,
    )

    # PauperCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards_pauper,
        MtgjsonAtomicCardsObject(card_format_map["pauper"]),
        pretty_print,
    )


def build_all_printings_files(pretty_print: bool) -> None:
    """
    Construct all entities that rely upon AllPrintings
    to avoid loading them into RAM too many times
    :param pretty_print: Pretty or minimal
    """
    all_printings = MtgjsonAllPrintingsObject()

    # AllPrintings.json
    create_compiled_output(
        MtgjsonStructuresObject().all_printings,
        all_printings.get_set_contents(),
        pretty_print,
    )

    # <FORMAT>.json
    build_format_specific_files(all_printings, pretty_print)

    # AllIdentifiers.json
    create_compiled_output(
        MtgjsonStructuresObject().all_identifiers,
        MtgjsonAllIdentifiersObject(all_printings.to_json()),
        pretty_print,
    )


def generate_sales_data_compiled_output(pretty_print: bool) -> None:
    """
    Create and dump TCGPlayer latest sales data output file
    :param pretty_print: Pretty or minimal
    """
    LOGGER.info("Building Latest Sales Data")
    if not MtgjsonConfig().output_path.joinpath("AllPrintings.json").exists():
        download_live_mtgjson_file("AllPrintings.json")
    if not MtgjsonConfig().output_path.joinpath("TcgplayerSkus.json").exists():
        download_live_mtgjson_file("TcgplayerSkus.json")

    create_compiled_output(
        MtgjsonStructuresObject().all_tcgplayer_sales,
        MtgjsonTcgplayerSalesHistoryObject(
            MtgjsonConfig().output_path.joinpath("AllPrintings.json"),
            MtgjsonConfig().output_path.joinpath("TcgplayerSkus.json"),
        ),
        pretty_print,
    )
    LOGGER.info("Finished Building Latest Sales Data")


def generate_compiled_output_files(pretty_print: bool) -> None:
    """
    Create and dump all compiled outputs
    :param pretty_print: Pretty or minimal
    """
    LOGGER.info("Building Compiled Outputs")

    # AllPrintings, <FORMAT>, & AllIdentifiers
    build_all_printings_files(pretty_print)

    # TcgplayerSkus.json
    create_compiled_output(
        MtgjsonStructuresObject().all_tcgplayer_skus,
        MtgjsonTcgplayerSkusObject(
            MtgjsonConfig().output_path.joinpath("AllPrintings.json")
        ),
        pretty_print,
    )

    # AllPrices.json
    generate_compiled_prices_output(build_prices(), pretty_print)

    # CompiledList.json
    create_compiled_output(
        MtgjsonStructuresObject().compiled_list,
        MtgjsonCompiledListObject(),
        pretty_print,
    )

    # Keywords.json
    create_compiled_output(
        MtgjsonStructuresObject().key_words,
        MtgjsonKeywordsObject(),
        pretty_print,
    )

    # CardTypes.json
    create_compiled_output(
        MtgjsonStructuresObject().card_types,
        MtgjsonCardTypesObject(),
        pretty_print,
    )

    # Meta.json (Formerly version.json)
    create_compiled_output(
        MtgjsonStructuresObject().version,
        MtgjsonMetaObject(),
        pretty_print,
    )

    # SetList.json
    create_compiled_output(
        MtgjsonStructuresObject().set_list, MtgjsonSetListObject(), pretty_print
    )

    # AtomicCards.json
    create_compiled_output(
        MtgjsonStructuresObject().atomic_cards,
        MtgjsonAtomicCardsObject(),
        pretty_print,
    )

    # <FORMAT>Atomic.json
    build_atomic_specific_files(pretty_print)

    # All Pre-constructed Decks
    deck_names = []
    for mtgjson_deck_obj in GitHubDecksProvider().iterate_precon_decks():
        mtgjson_deck_header_obj = MtgjsonDeckHeaderObject(mtgjson_deck_obj)
        create_compiled_output(
            f"decks/{mtgjson_deck_header_obj.file_name}",
            mtgjson_deck_obj,
            pretty_print,
        )
        deck_names.append(mtgjson_deck_header_obj)

    # DeckList.json
    create_compiled_output(
        MtgjsonStructuresObject().deck_list,
        MtgjsonDeckListObject(deck_names),
        pretty_print,
    )

    # EnumValues.json - Depends on Keywords & Decks
    create_compiled_output(
        MtgjsonStructuresObject().enum_values,
        MtgjsonEnumValuesObject(),
        pretty_print,
    )


def create_compiled_output(
    compiled_name: str, compiled_object: Any, pretty_print: bool, sort_keys: bool = True
) -> None:
    """
    Log and write out a compiled output file
    :param compiled_name: What file to save
    :param compiled_object: What content to write
    :param pretty_print: Pretty or minimal
    :param sort_keys: Sort the data keys before dumping
    """
    LOGGER.info(f"Generating {compiled_name}")
    write_to_file(compiled_name, compiled_object, pretty_print, sort_keys)
    LOGGER.debug(f"Finished Generating {compiled_name}")


def construct_format_map(
    all_printings_path: pathlib.Path = MtgjsonConfig().output_path.joinpath(
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
        magic_format: [] for magic_format in constants.SUPPORTED_FORMAT_OUTPUTS
    }

    if not all_printings_path.is_file():
        LOGGER.warning(f"{all_printings_path} was not found, skipping format map")
        return {}

    with all_printings_path.open(encoding="utf-8") as file:
        content = json.load(file)

    for set_code_key, set_code_content in content.get("data", {}).items():
        if (
            normal_sets_only
            and set_code_content.get("type") not in constants.SUPPORTED_SET_TYPES
        ):
            continue

        formats_set_legal_in = constants.SUPPORTED_FORMAT_OUTPUTS
        for card in set_code_content.get("cards"):
            # Don't include Alchemy cards in determining legality
            if card.get("name", "").startswith("A-"):
                continue

            card_legalities = set(card.get("legalities").keys())
            formats_set_legal_in = formats_set_legal_in.intersection(card_legalities)

        for magic_format in formats_set_legal_in:
            format_map[magic_format].append(set_code_key)

    return format_map


def construct_atomic_cards_format_map(
    all_printings_path: pathlib.Path = MtgjsonConfig().output_path.joinpath(
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
    format_card_map: Dict[str, List[Dict[str, Any]]] = {
        magic_format: [] for magic_format in constants.SUPPORTED_FORMAT_OUTPUTS
    }

    if not all_printings_path.is_file():
        LOGGER.warning(f"{all_printings_path} was not found, skipping format map")
        return {}

    with all_printings_path.open(encoding="utf-8") as file:
        content = json.load(file)

    for set_contents in content.get("data", {}).values():
        set_cards: List[Dict[str, Any]] = set_contents.get("cards", [])

        # Workaround for Dungeons so they can be included
        for token in set_contents.get("tokens", []):
            if token.get("type") == "Dungeon":
                token["legalities"] = {
                    t_format: "Legal" for t_format in format_card_map.keys()
                }
                set_cards.append(token)

        for card in set_cards:
            for magic_format in format_card_map.keys():
                if card.get("legalities", {}).get(magic_format) in {
                    "Legal",
                    "Restricted",
                }:
                    format_card_map[magic_format].append(card)

    return format_card_map


def generate_output_file_hashes(directory: pathlib.Path) -> None:
    """
    Given a directory, hash each file within it and write that hash
    out to the file "FILENAME.HASH_NAME"
    :param directory: Directory to hash
    """
    for file in directory.glob("**/*"):
        if file.is_dir():
            continue

        # Don't hash the hash file...
        if file.name.endswith(constants.HASH_TO_GENERATE.name):
            continue

        generated_hash = get_file_hash(file)
        if not generated_hash:
            continue

        hash_file_name = f"{file.name}.{constants.HASH_TO_GENERATE.name}"
        with file.parent.joinpath(hash_file_name).open(
            "w", encoding="utf-8"
        ) as hash_file:
            hash_file.write(generated_hash)


def write_to_file(
    file_name: str, file_contents: Any, pretty_print: bool, sort_keys: bool = True
) -> None:
    """
    Dump content to a file in the outputs directory
    :param file_name: File to dump to
    :param file_contents: Contents to dump
    :param pretty_print: Pretty or minimal
    :param sort_keys: Should data keys be sorted
    """
    write_file = MtgjsonConfig().output_path.joinpath(f"{file_name}.json")
    write_file.parent.mkdir(parents=True, exist_ok=True)

    if sort_keys:
        # NOTE: Super large files will cause this to run out of memory
        file_contents = json.loads(
            json.dumps(
                file_contents,
                sort_keys=True,
                ensure_ascii=False,
                default=lambda o: o.to_json(),
            )
        )

    with write_file.open("w", encoding="utf-8") as file:
        json.dump(
            obj={"meta": MtgjsonMetaObject(), "data": file_contents},
            fp=file,
            indent=(4 if pretty_print else None),
            ensure_ascii=False,
            default=lambda o: o.to_json(),
        )
