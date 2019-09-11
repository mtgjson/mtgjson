import json
import logging

FORMATS = (
    'standard',
    'future',
    'modern',
    'legacy',
    'vintage',
    'commander',
    'brawl',
    'pauper',
    'penny',
    'oldschool',
    'duel'
)


logger = logging.getLogger(__name__)


class UnrecognizedFormatException(Exception):
    pass  # TODO: Define MTGJSON exception hierarchy.


def find_formats_for_sets(all_sets_path):
    """
    For each set in the specified JSON file, determine its legal sets and return a dictionary mapping set code to
    a list of legal formats.

    :param all_sets_path: Path to AllSets.json file
    :type all_sets_path: str

    :return: Dictionary of the form { code: [formats] }
    :rtype: dict
    """
    try:
        with open(all_sets_path, encoding='utf-8', mode='r') as all_sets_json:
            all_sets = json.load(all_sets_json)
            set_fmts = {}

            for code, data in all_sets.items():
                possible_fmts = set(FORMATS)
                cards = data.get('cards')

                for card in cards:
                    card_fmts = set(card.get('legalities').keys())
                    possible_fmts &= card_fmts

                set_fmts[code] = possible_fmts

            return set_fmts

    except IOError:
        logger.exception("Could not open {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.

    except json.JSONDecodeError:
        logger.exception("Could not decode JSON from {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.
