"""
MTGJSON Arg Parser to determine what actions to run
"""
import argparse
import pathlib
from typing import List

from .consts import BAD_FILE_NAMES, OUTPUT_PATH
from .providers import ScryfallProvider
from .utils import get_thread_logger

LOGGER = get_thread_logger()


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments from user to determine how to spawn up
    MTGJSON and complete the request.
    :return: Namespace of requests
    """
    parser = argparse.ArgumentParser("mtgjson5")

    # What set(s) to build
    sets_group = parser.add_mutually_exclusive_group()
    sets_group.add_argument(
        "-s",
        "--sets",
        type=str.upper,
        nargs="*",
        metavar="SET",
        default=[],
        help="What set(s) to build",
    )
    sets_group.add_argument(
        "-a", "--all-sets", action="store_true", help="Alias to build all sets"
    )

    parser.add_argument(
        "-c", "--full-build", action="store_true", help="Compile extra outputs"
    )
    parser.add_argument(
        "-x",
        "--resume-build",
        action="store_true",
        help="Continue from last set built in outputs folder",
    )
    parser.add_argument(
        "-z", "--compress", action="store_true", help="Compress outputs folder contents"
    )
    parser.add_argument(
        "-p",
        "--pretty",
        action="store_true",
        help="Outputs will be prettified over minified",
    )
    parser.add_argument(
        "-m", "--pricing", action="store_true", help="Compile only pricing files"
    )

    parser.add_argument(
        "--skip-keys",
        "--no-keys",
        action="store_true",
        help="Disable privileged information lookups for builds",
    )
    parser.add_argument(
        "--skip-sets",
        "--no-sets",
        type=str.upper,
        nargs="*",
        metavar="SET",
        default=[],
        help="Purposely exclude sets from the build that may have been set using '--sets' or '--all'",
    )

    return parser.parse_args()


def get_sets_already_built() -> List[str]:
    """
    Grab sets that have already been compiled by the system
    :return: List of all set codes found
    """
    json_output_files: List[pathlib.Path] = list(OUTPUT_PATH.glob("**/*.json"))

    set_codes_found = [file.stem for file in json_output_files]
    LOGGER.info(set_codes_found)

    set_codes_found = [
        set_code[:-1] if set_code[:-1] in BAD_FILE_NAMES else set_code
        for set_code in set_codes_found
    ]

    return set_codes_found


def get_all_scryfall_sets() -> List[str]:
    """
    Grab all sets that Scryfall currently supports
    :return: Scryfall sets
    """
    scryfall_instance = ScryfallProvider()
    scryfall_sets = scryfall_instance.download(scryfall_instance.ALL_SETS_URL)

    if scryfall_sets["object"] == "error":
        LOGGER.error(f"Downloading Scryfall data failed: {scryfall_sets}")
        return []

    # Get _ALL_ Scryfall sets
    return [set_obj["code"].upper() for set_obj in scryfall_sets["data"]]


def get_sets_to_build(args: argparse.Namespace) -> List[str]:
    """
    Grab what sets to build given build params
    :param args: CLI args
    :return: List of sets to construct, alphabetically
    """
    if args.resume_build:
        # Exclude sets we have already built
        args.skip_sets.extend(get_sets_already_built())

    if not args.all_sets:
        # We have a sub-set list, so only return what we want
        return sorted(list(set(args.sets) - set(args.skip_sets)))

    scryfall_sets = get_all_scryfall_sets()

    # Remove Scryfall token sets (but leave extra sets)
    non_token_sets = {
        s for s in scryfall_sets if not (s.startswith("T") and s[1:] in scryfall_sets)
    }

    # Remove sets to skip
    return_list = list(non_token_sets - set(args.skip_sets))

    return sorted(return_list)
