import aiohttp
import argparse
import asyncio
import copy
import itertools
import json
from mtgjson4 import mtg_builder, mtg_global, mtg_storage
import os
import pathlib
import sys
import time
from typing import Iterator, List

from mtgjson4 import stacktracer  # TEMPORARY

stacktracer.trace_start("trace.html")


async def main(loop: asyncio.AbstractEventLoop, session: aiohttp.ClientSession, language_to_build: str,
               args: dict) -> None:
    """
    Main method that starts the entire build process
    :param args:
    :param loop:
    :param session:
    :param language_to_build:
    :return:
    """

    def get_next_batch_of_sets(queue: Iterator[List[str]]) -> List[List[str]]:
        max_pops = int(args['max_sets_build'][0])

        # User disabled this memory protection feature
        if max_pops == 0:
            return list(queue)
        return list(itertools.islice(queue, max_pops))

    mtg_storage.ensure_set_dir_exists()

    sets_queue = iter(SETS_TO_BUILD)
    async with session:
        # Start asyncio tasks for building each set
        json_builder = mtg_builder.MTGJSON(SETS_TO_BUILD, session, loop)

        # We will only be building a few sets at a time, to allow for partial outputs
        sets_to_build_now = get_next_batch_of_sets(sets_queue)
        while len(sets_to_build_now) > 0:
            # Create our builders for the few sets
            futures = [
                loop.create_task(json_builder.build_set(set_name, language_to_build)) for set_name in sets_to_build_now
            ]

            # Then wait until all of them are completed
            await asyncio.wait(futures)

            # Then queue up our next sets to build
            sets_to_build_now = get_next_batch_of_sets(sets_queue)

    # And we're done! :)
    return


# This is a raw transposition from MTGJSONv3...
# I have no idea if it's correct or not.
def create_all_sets_files():
    mtg_storage.COMP_OUT_DIR.mkdir(exist_ok=True)

    # Set Variables
    all_sets = dict()
    all_sets_with_extras = dict()
    all_sets_array = list()
    all_sets_array_with_extras = list()

    # Cards Variables
    # all_cards = dict()
    all_cards_with_extras = dict()

    # Other Stuff
    previous_seen_set_codes = dict()
    tainted_cards = list()
    ignored_sets = ['UGL', 'UST', 'UNH']

    def ready_to_diff(obj):
        if isinstance(obj, list):
            return ' '.join([ready_to_diff(o) for o in obj])

        if isinstance(obj, dict):
            keys = sorted(obj.keys())
            arr = [str({key: ready_to_diff(obj[key])}) for key in keys]
            return ' '.join(arr)

        return obj

    def process_card(card_set, card):
        if card['name'] not in all_cards_with_extras:
            all_cards_with_extras[card['name']] = dict()

        if card['name'] not in previous_seen_set_codes:
            previous_seen_set_codes[card['name']] = dict()

        def check_taint(a_field_name, a_field_value=None):
            if card_set['code'] in ignored_sets:
                return

            if a_field_value is None:
                if a_field_name in card:
                    a_field_value = card[a_field_name]

            if a_field_name not in all_cards_with_extras[card['name']]:
                return

            previous_value = all_cards_with_extras[card['name']][a_field_name]

            taint = False

            if previous_value:
                if a_field_value is None:
                    taint = True
                else:
                    prev = ready_to_diff(previous_value)
                    field = ready_to_diff(a_field_value)

                    if prev != field:
                        taint = True

            if taint:
                tainted_cards.append({'card': card, 'fieldName': a_field_name})

        for field_name in mtg_global.FIELD_TYPES.keys():
            if field_name in mtg_global.SET_SPECIFIC_FIELDS:
                continue

            if field_name not in previous_seen_set_codes[card['name']]:
                previous_seen_set_codes[card['name']][field_name] = list()

            if field_name in card.keys():
                field_value = card[field_name]

                if field_name in mtg_global.ORACLE_FIELDS and field_name != 'foreignNames':
                    check_taint(field_name, field_value)

                previous_seen_set_codes[card['name']][field_name].append(card_set['code'])
                all_cards_with_extras[card['name']][field_name] = field_value

        return card

    def process_set(sets):
        for a_set in sets:
            for card in a_set['cards']:
                process_card(a_set, card)

            a_set.pop('isMCISet', None)
            a_set.pop('magicRaritiesCode', None)
            a_set.pop('essentialMagicCode', None)
            a_set.pop('useMagicRaritiesNumber', None)

        simple_set = copy.copy(sets)
        for b_set in simple_set:
            for simple_set_card in b_set['cards']:
                for unneeded_field in mtg_global.EXTRA_FIELDS:
                    simple_set_card.pop(unneeded_field, None)

        return [sets, simple_set]

    # LoadJSON
    sets_in_output = list()
    for file in os.listdir(mtg_storage.SET_OUT_DIR):
        with pathlib.Path(mtg_storage.SET_OUT_DIR, file).open('r', encoding='utf-8') as fp:
            file_content = json.load(fp)
            sets_in_output.append(file_content)

    # ProcessJSON
    params = {'sets': {}}

    for set_data in sets_in_output:
        params['sets'][set_data['code']] = {'code': set_data['code'], 'releaseDate': set_data['releaseDate']}

        full_simple_sets = process_set(sets_in_output)

        all_sets_with_extras[set_data['code']] = full_simple_sets[0]
        all_sets_array_with_extras.append(full_simple_sets[0])
        all_sets[set_data['code']] = full_simple_sets[1]
        all_sets_array.append(full_simple_sets[1])

    # saveFullJSON
    def save(f_name, data):
        with (mtg_storage.COMP_OUT_DIR / '{}.json'.format(f_name)).open('w', encoding='utf-8') as save_fp:
            json.dump(data, save_fp, indent=4, sort_keys=True, ensure_ascii=False)
        return len(data)

    all_cards = copy.copy(all_cards_with_extras)
    for card_keys in all_cards.keys():
        for extra_field in mtg_global.EXTRA_FIELDS:
            if extra_field in card_keys:
                card_keys.remove(extra_field)

    data_block = {
        'AllSets': {
            'data': all_sets,
            'param': 'allSize'
        },
        'AllSets-x': {
            'data': all_sets_with_extras,
            'param': 'allSizeX'
        },
        'AllSetsArray': {
            'data': all_sets_array,
            'param': 'allSizeArray'
        },
        'AllSetsArray-x': {
            'data': all_sets_array_with_extras,
            'param': 'allSizeArrayX'
        },
        'AllCards': {
            'data': all_cards,
            'param': 'allCards'
        },
        'AllCards-x': {
            'data': all_cards_with_extras,
            'param': 'allCardsX'
        }
    }

    for block in data_block:
        save(block, data_block[block]['data'])


if __name__ == '__main__':
    # Start by processing all arguments to the program
    parser = argparse.ArgumentParser(description=mtg_global.DESCRIPTION)

    parser.add_argument('-v', '--version', action='store_true', help='MTGJSON version information')

    parser.add_argument(
        '-s',
        '--sets',
        metavar='SET',
        nargs='+',
        type=str,
        help='A list of sets to build. Will be ignored if used with --all-sets.')

    parser.add_argument(
        '-a',
        '--all-sets',
        action='store_true',
        help='Build all sets found in the set_configs directory, including sub-directories.')

    parser.add_argument(
        '-f',
        '--full-out',
        action='store_true',
        help='Create the AllCards, AllSets, and AllSetsArray files based on the sets found in the set_outputs directory.'
    )

    parser.add_argument(
        '-l',
        '--language',
        default=['en'],
        metavar='LANG',
        type=str,
        nargs=1,
        help=
        'Build foreign language version of a specific set. The english version must have been built already for this flag to be used.'
    )

    parser.add_argument(
        '--max-sets-build',
        default=[5],
        metavar='#',
        type=int,
        nargs=1,
        help=
        'You can limit how many sets will be built at one time. The higher the number, the more memory consumption. If not enough memory, swap space will be used, which slows down the program tremendiously. Defaults to 5. 0 to Disable.'
    )

    # If user supplies no arguments, show help screen and exit
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        exit(1)

    cl_args = vars(parser.parse_args())
    lang_to_process = cl_args['language'][0]

    # Get version info and exit
    if cl_args['version']:
        print(mtg_global.VERSION_INFO)
        exit(0)

    # Ensure the language is a valid language, otherwise exit
    if mtg_global.get_language_long_name(lang_to_process) is None:
        print('MTGJSON: Language \'{}\' not supported yet'.format(lang_to_process))
        exit(1)

    # If only full out, just build from what's there and exit
    if (cl_args['sets'] is None) and (not cl_args['all_sets']) and cl_args['full_out']:
        create_all_sets_files()
        exit(0)

    # Global of all sets to build
    SETS_TO_BUILD = mtg_builder.determine_gatherer_sets(cl_args)

    # Start the build process
    start_time = time.time()

    card_loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    card_session = aiohttp.ClientSession(loop=card_loop, raise_for_status=True, conn_timeout=None, read_timeout=None)
    card_loop.run_until_complete(main(card_loop, card_session, lang_to_process, cl_args))

    if cl_args['full_out']:
        create_all_sets_files()

    end_time = time.time()
    print('Time: {}'.format(end_time - start_time))
