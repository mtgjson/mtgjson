import argparse
import asyncio
import itertools
import sys
import time
from typing import Iterator, List

import aiohttp

from mtgjson4 import mtg_builder, mtg_global, mtg_storage

try:
    import hanging_threads
    THREAD_MONITOR = hanging_threads.start_monitoring()
except ImportError:
    print('INFO: hanging_threads not installed - Thread Monitor is not running.')


async def main(loop: asyncio.AbstractEventLoop, session: aiohttp.ClientSession, args: dict) -> None:
    """
    Main method that starts the entire build process
    :param args:
    :param loop:
    :param session:
    :return:
    """

    def get_next_batch_of_sets(queue: Iterator[List[str]]) -> List[List[str]]:
        """
        To ensure better performance, we limit the number of sets built at a time
        to limit our memory impact. This will return the next group of sets to
        build.
        """
        max_pops = int(args['max_sets_build'][0])

        # User disabled this memory protection feature
        if max_pops == 0:
            return list(queue)
        return list(itertools.islice(queue, max_pops))

    # Main Applied
    mtg_storage.ensure_set_dir_exists()

    sets_queue = iter(SETS_TO_BUILD)
    async with session:
        # Start asyncio tasks for building each set
        json_builder = mtg_builder.MTGJSON(SETS_TO_BUILD, session, loop)

        # We will only be building a few sets at a time, to allow for partial outputs
        sets_to_build_now = get_next_batch_of_sets(sets_queue)
        while sets_to_build_now:
            # Create our builders for the few sets
            futures = [loop.create_task(json_builder.build_set(set_name)) for set_name in sets_to_build_now]

            # Then wait until all of them are completed
            await asyncio.wait(futures)

            # Then queue up our next sets to build
            sets_to_build_now = get_next_batch_of_sets(sets_queue)

    # And we're done! :)
    return


if __name__ == '__main__':
    # Start by processing all arguments to the program
    arg_parser = argparse.ArgumentParser(description=mtg_global.DESCRIPTION)

    arg_parser.add_argument('-v', '--version', action='store_true', help='MTGJSON version information')

    arg_parser.add_argument(
        '-s',
        '--sets',
        metavar='SET',
        nargs='+',
        type=str,
        help='A list of sets to build. Will be ignored if used with --all-sets.')

    arg_parser.add_argument(
        '-a',
        '--all-sets',
        action='store_true',
        help='Build all sets found in the set_configs directory, including sub-directories.')

    arg_parser.add_argument(
        '-f',
        '--full-out',
        action='store_true',
        help='Create the AllCards, AllSets, and AllSetsArray files based on the sets found in the set_outputs '
        'directory. ')

    arg_parser.add_argument(
        '--max-sets-build',
        default=[5],
        metavar='#',
        type=int,
        nargs=1,
        help='You can limit how many sets will be built at one time. The higher the number, the more memory '
        'consumption. If not enough memory, swap space will be used, which slows down the program tremendiously. '
        'Defaults to 5. 0 to Disable. ')

    # If user supplies no arguments, show help screen and exit
    if len(sys.argv) == 1:
        arg_parser.print_help(sys.stderr)
        exit(1)

    cl_args = vars(arg_parser.parse_args())

    # Get version info and exit
    if cl_args['version']:
        print(mtg_global.VERSION_INFO)
        exit(0)

    # If only full out, just build from what's there and exit
    if (cl_args['sets'] is None) and (not cl_args['all_sets']) and cl_args['full_out']:
        mtg_builder.create_combined_outputs()
        exit(0)

    # Global of all sets to build
    SETS_TO_BUILD = mtg_builder.determine_gatherer_sets(cl_args)

    # Start the build process
    start_time = time.time()

    card_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    # card_loop.set_debug(enabled=True)
    card_session = aiohttp.ClientSession(
        loop=card_loop,
        conn_timeout=60,
        read_timeout=60,
        raise_for_status=True,
        connector=aiohttp.TCPConnector(limit=200))
    card_loop.run_until_complete(main(card_loop, card_session, cl_args))

    if cl_args['full_out']:
        mtg_builder.create_combined_outputs()

    end_time = time.time()
    print('Time: {}'.format(end_time - start_time))
