"""
MTGJSON Arg Parser to determine what actions to take
"""

import argparse
import logging
import os
import sys

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments from user to determine how to spawn up
    MTGJSON and complete the request.
    :return: Namespace of requests
    """
    parser = argparse.ArgumentParser("mtgjson5")

    parser.add_argument(
        "--use-envvars",
        action="store_true",
        help="Use environment variables over parser flags for build operations",
    )

    # What set(s) to build
    def parse_sets(s: str) -> list[str]:
        """Parse set codes, handling both comma and space separation."""
        return [code.strip().upper() for code in s.split(",") if code.strip()]

    sets_group = parser.add_mutually_exclusive_group()
    sets_group.add_argument(
        "--sets",
        "-s",
        type=lambda s: s.upper(),
        nargs="*",
        metavar="SET",
        default=[],
        help="Set(s) to build, using Scryfall set code notation. Non-existent sets silently ignored.",
    )
    sets_group.add_argument(
        "--all-sets",
        "-a",
        action="store_true",
        help="Build all possible sets, overriding the --sets option.",
    )

    parser.add_argument(
        "--full-build",
        "-c",
        action="store_true",
        help="Build new prices, MTGSQLive, and compiled outputs like AllPrintings.",
    )
    parser.add_argument(
        "--resume-build",
        "-x",
        action="store_true",
        help="While determining what sets to build, ignore individual set files found in the output directory.",
    )
    parser.add_argument(
        "--compress",
        "-z",
        action="store_true",
        help="Compress the output folder's contents for distribution.",
    )
    parser.add_argument(
        "--pretty",
        "-p",
        action="store_true",
        help="When dumping JSON files, prettify the contents instead of minifying them.",
    )
    parser.add_argument(
        "--skip-sets",
        "-SS",
        type=lambda s: s.upper(),
        nargs="*",
        metavar="SET",
        default=[],
        help="Purposely exclude sets from the build that may have been set using --sets or --all-sets. Supports comma or space separation.",
    )
    parser.add_argument(
        "--polars",
        action="store_true",
        help="Enables Polars based high performance pipeline.",
    )
    parser.add_argument(
        "--bulk-files",
        "-BF",
        action="store_true",
        help="Enable the use of Scryfall bulk data files where possible.",
    )

    mtgjson_arg_group = parser.add_argument_group("mtgjson maintainer arguments")
    mtgjson_arg_group.add_argument(
        "--price-build",
        "-PB",
        action="store_true",
        help="Build updated pricing data then exit.",
    )
    mtgjson_arg_group.add_argument(
        "--referrals",
        "-R",
        action="store_true",
        help="Create and maintain a referral map for referral linkages.",
    )
    mtgjson_arg_group.add_argument(
        "--no-alerts",
        "-NA",
        action="store_true",
        help="Prevent push notifications from sending when property keys are defined.",
    )
    mtgjson_arg_group.add_argument(
        "--aws-ssm-download-config",
        type=str,
        metavar="CONFIG_NAME",
        help="AWS Parameter Store config name to load in, if local config file is not wanted/available.",
    )
    mtgjson_arg_group.add_argument(
        "--aws-s3-upload-bucket",
        type=str,
        metavar="BUCKET_NAME",
        help="Upload finished results to an S3 bucket.",
    )

    # Show help menu if no arguments are passed
    if len(sys.argv) == 1:
        parser.print_help()
        parser.exit()

    parsed_args = parser.parse_args()

    if parsed_args.sets:
        flattened_sets = []
        for item in parsed_args.sets:
            if isinstance(item, list):
                flattened_sets.extend(item)
            else:
                flattened_sets.append(item.upper())
        parsed_args.sets = flattened_sets

    if parsed_args.use_envvars:
        LOGGER.info("Using environment variables over parser flags")
        parsed_args.sets = list(filter(None, os.environ.get("SETS", "").split(",")))
        parsed_args.all_sets = bool(os.environ.get("ALL_SETS", False))
        parsed_args.full_build = bool(os.environ.get("FULL_BUILD", False))
        parsed_args.resume_build = bool(os.environ.get("RESUME_BUILD", False))
        parsed_args.compress = bool(os.environ.get("COMPRESS", False))
        parsed_args.pretty = bool(os.environ.get("PRETTY", False))
        parsed_args.polars = bool(os.environ.get("POLARS", False))
        parsed_args.bulk_files = bool(os.environ.get("USE_BULK", False))
        parsed_args.skip_sets = list(
            filter(None, os.environ.get("SKIP_SETS", "").split(","))
        )
        parsed_args.price_build = bool(os.environ.get("PRICE_BUILD", False))
        parsed_args.referrals = bool(os.environ.get("REFERRALS", False))
        parsed_args.no_alerts = bool(os.environ.get("NO_ALERTS", False))
        parsed_args.aws_ssm_download_config = os.environ.get("AWS_SSM_DOWNLOAD_CONFIG")
        parsed_args.aws_s3_upload_bucket = os.environ.get("AWS_S3_UPLOAD_BUCKET")

    return parsed_args
