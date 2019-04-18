"""MTGJSON Version 4 Compiler"""
import argparse
import configparser
import logging
import os
import pathlib
import sys
from typing import Any, Dict, List

import mtgjson4
from mtgjson4 import compile_mtg, outputter
from mtgjson4.provider import scryfall
import mtgjson4.util

LOGGER = logging.getLogger(__name__)


def get_all_sets() -> List[str]:
    """
    Grab the set codes (~3 letters) for all sets found
    in the config database.
    :return: List of all set codes found, sorted
    """
    downloaded = mtgjson4.provider.scryfall.download(scryfall.SCRYFALL_API_SETS)
    if downloaded["object"] == "error":
        LOGGER.error("Downloading Scryfall data failed: {}".format(downloaded))
        return []

    # Get _ALL_ Scryfall sets
    set_codes: List[str] = [set_obj["code"] for set_obj in downloaded["data"]]

    # Remove Scryfall token sets (but leave extra sets)
    set_codes = [s for s in set_codes if not (s.startswith("t") and s[1:] in set_codes)]

    return sorted(set_codes)


def get_compiled_sets() -> List[str]:
    """
    Grab the official set codes for all sets that have already been
    compiled and are awaiting use in the set_outputs dir.
    :return: List of all set codes found
    """
    all_paths: List[pathlib.Path] = list(mtgjson4.COMPILED_OUTPUT_DIR.glob("**/*.json"))
    all_sets_found: List[str] = [
        str(card_set).split("/")[-1][:-5].lower() for card_set in all_paths
    ]

    all_sets_found = [
        x[:-1] if x[:-1].upper() in mtgjson4.BANNED_FILE_NAMES else x
        for x in all_sets_found
    ]

    return all_sets_found


def init_mkm_const() -> None:
    """
    MKM SDK requires global variables, so this sets them
    up before we start the system
    """
    # MKM Globals
    if mtgjson4.CONFIG_PATH.is_file():
        # Open and read MTGJSON secret properties
        config = configparser.RawConfigParser()
        config.read(mtgjson4.CONFIG_PATH)
        os.environ["MKM_APP_TOKEN"] = config.get("CardMarket", "app_token")
        os.environ["MKM_APP_SECRET"] = config.get("CardMarket", "app_secret")


def main() -> None:
    """
    Main Method
    """
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-a", action="store_true")
    parser.add_argument("-s", metavar="SET", nargs="*", type=str)
    parser.add_argument("-c", action="store_true")
    parser.add_argument("-x", action="store_true")
    parser.add_argument("--skip-tcgplayer", action="store_true")
    parser.add_argument("--skip-sets", metavar="SET", nargs="*", type=str)
    parser.add_argument("--skip-cache", action="store_true")

    # Ensure there are args
    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)
    else:
        args = parser.parse_args()

    if not mtgjson4.CONFIG_PATH.is_file():
        LOGGER.warning(
            "No properties file found at {}. Will download without authentication".format(
                mtgjson4.CONFIG_PATH
            )
        )

    mtgjson4.USE_CACHE.set(not args.skip_cache)

    # Determine set(s) to build
    args_s = args.s if args.s else []
    set_list: List[str] = get_all_sets() if args.a else args_s

    if args.skip_sets:
        set_list = list(set(set_list) - set(args.skip_sets))
        LOGGER.info("Skipping set(s) by request of user: {}".format(args.skip_sets))

    LOGGER.info("Sets to compile: {}".format(set_list))

    # If we had to kill mid-build, we can skip the completed set(s)
    if args.x:
        sets_compiled_already: List[str] = get_compiled_sets()
        set_list = [s for s in set_list if s not in sets_compiled_already]
        LOGGER.info(
            "Sets to skip compilation for: {}\n\nSets to compile, after cached sets removed: {}".format(
                sets_compiled_already, set_list
            )
        )

    for set_code in set_list:
        sf_set: List[Dict[str, Any]] = scryfall.get_set(set_code)
        compiled = compile_mtg.build_output_file(sf_set, set_code, args.skip_tcgplayer)

        # If we have at least 1 card, dump to file SET.json
        # but first add them to ReferralMap.json
        if compiled["cards"] or compiled["tokens"]:
            if not args.skip_tcgplayer:
                for card in compiled["cards"]:
                    # ReferralMap TCGPlayer
                    key_tcg = mtgjson4.util.url_keygen(card.get("tcgplayerProductId"))
                    # ReferralMap CardMarket
                    key_mkm = mtgjson4.util.url_keygen(
                        int(
                            str(card.get("mcmId"))
                            + "10101"  # Buffer to distinguish from each other & TCGPlayer
                            + str(card.get("mcmMetaId"))
                        )
                    )

                    outputter.write_referral_url_information(
                        {
                            key_mkm: card.get_card_market_url(),
                            key_tcg: card.get_tcgplayer_url(),
                        }
                    )

            mtgjson4.outputter.write_to_file(set_code.upper(), compiled, set_file=True)

    # Compile the additional outputs
    if args.c:
        LOGGER.info("Compiling Additional Outputs")
        mtgjson4.outputter.create_and_write_compiled_outputs()


if __name__ == "__main__":
    mtgjson4.init_logger()
    init_mkm_const()
    main()
