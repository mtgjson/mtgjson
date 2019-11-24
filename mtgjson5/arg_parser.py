"""
MTGJSON Arg Parser to determine what actions to run
"""
import argparse


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments from user to determine how to spawn up
    MTGJSON and complete the request.
    :return: Namespace of requests
    """
    parser = argparse.ArgumentParser("mtgjson4")

    # What set(s) to build
    sets_group = parser.add_mutually_exclusive_group(required=False)
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
    parser.add_argument(
        "--skip-cache",
        "--no-cache",
        action="store_true",
        help="Prevent the caching of data from external sources",
    )

    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
