"""
MTGJSON Main Executor
"""
import gevent.monkey  # isort:skip

gevent.monkey.patch_all()  # isort:skip

import argparse
import logging
import traceback
from typing import List, Set, Union

from mtgjson5 import constants
from mtgjson5.utils import init_logger


def build_mtgjson_sets(
    sets_to_build: Union[Set[str], List[str]],
    output_pretty: bool,
    include_referrals: bool,
) -> None:
    """
    Build each set one-by-one and output them to a file
    :param sets_to_build: Sets to construct
    :param output_pretty: Should we dump minified
    :param include_referrals: Should we include referrals
    """
    from mtgjson5.output_generator import write_to_file
    from mtgjson5.providers import WhatsInStandardProvider
    from mtgjson5.referral_builder import (
        build_and_write_referral_map,
        fixup_referral_map,
    )
    from mtgjson5.set_builder import build_mtgjson_set

    LOGGER.info(f"Building {len(sets_to_build)} Sets: {', '.join(sets_to_build)}")

    # Prime WhatsInStandard lookup
    _ = WhatsInStandardProvider().standard_legal_set_codes

    for set_to_build in sets_to_build:
        # Build the full set
        mtgjson_set = build_mtgjson_set(set_to_build)
        if not mtgjson_set:
            continue

        # Handle referral components
        if include_referrals:
            build_and_write_referral_map(mtgjson_set)

        # Dump set out to file
        write_to_file(
            file_name=mtgjson_set.get_windows_safe_set_code(),
            file_contents=mtgjson_set,
            pretty_print=output_pretty,
        )

    if sets_to_build and include_referrals:
        fixup_referral_map()


def validate_config_file_in_place() -> None:
    """
    Check to see if the MTGJSON config file was found.
    If not, kill the system with an error message.
    """
    if not constants.CONFIG_PATH.exists():
        LOGGER.error(
            f"{constants.CONFIG_PATH.name} was not found ({constants.CONFIG_PATH}). "
            "Please create this file and re-run the program. "
            "You can copy paste the example file into the "
            "correct location and (optionally) fill in your keys."
        )
        raise ValueError("ConfigPath not found")


def dispatcher(args: argparse.Namespace) -> None:
    """
    MTGJSON Dispatcher
    """
    from mtgjson5.compress_generator import compress_mtgjson_contents
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.mtgjson_s3_handler import MtgjsonS3Handler
    from mtgjson5.output_generator import (
        build_price_files,
        generate_compiled_output_files,
        generate_output_file_hashes,
    )
    from mtgjson5.providers import GitHubMTGSqliteProvider, ScryfallProvider

    # If a price build, simply build prices and exit
    if args.price_build:
        build_price_files(args.pretty)
        if args.compress:
            compress_mtgjson_contents(MtgjsonConfig().output_path)
        generate_output_file_hashes(MtgjsonConfig().output_path)
        return

    sets_to_build = ScryfallProvider().get_sets_to_build(args)
    if sets_to_build:
        build_mtgjson_sets(sets_to_build, args.pretty, args.referrals)

    if args.full_build:
        generate_compiled_output_files(args.pretty)
        GitHubMTGSqliteProvider().build_sql_and_csv_files()

    if args.compress:
        compress_mtgjson_contents(MtgjsonConfig().output_path)
    generate_output_file_hashes(MtgjsonConfig().output_path)

    if args.aws_s3_upload_bucket:
        MtgjsonS3Handler().upload_directory(
            MtgjsonConfig().output_path, args.aws_s3_upload_bucket, {"Prunable": "true"}
        )


def main() -> None:
    """
    MTGJSON safe main call
    """
    from mtgjson5.arg_parser import parse_args
    from mtgjson5.mtgjson_config import MtgjsonConfig
    from mtgjson5.utils import send_push_notification

    args = parse_args()
    if args.aws_ssm_download_config:
        MtgjsonConfig(args.aws_ssm_download_config)
    else:
        validate_config_file_in_place()
        MtgjsonConfig()

    LOGGER.info(
        f"Starting {MtgjsonConfig().mtgjson_version} on {constants.MTGJSON_BUILD_DATE}"
    )

    try:
        if not args.no_alerts:
            send_push_notification(f"Starting build\n{args}")
        dispatcher(args)
        if not args.no_alerts:
            send_push_notification("Build finished")
    except Exception as error:
        LOGGER.fatal(f"Exception caught: {error} {traceback.format_exc()}")
        if not args.no_alerts:
            send_push_notification(f"Build failed: {error}\n{traceback.format_exc()}")


if __name__ == "__main__":
    init_logger()
    LOGGER: logging.Logger = logging.getLogger(__name__)
    main()
