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
            set_formats = {}

            for code, data in all_sets.items():
                possible_formats = set(FORMATS)
                cards = data.get('cards')

                for card in cards:
                    card_formats = set(card.get('legalities').keys())
                    possible_formats &= card_formats

                set_formats[code] = possible_formats

            return set_formats

    except IOError:
        logger.exception("Could not open {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.

    except json.JSONDecodeError:
        logger.exception("Could not decode JSON from {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.


def build_format_subset(target_format, format_map):
    """
    Given a format and a format mapping, produce a subset of sets that are legal in this format.

    :param target_format: The format to find sets for
    :type target_format: str
    :param format_map: Mapping between set codes and formats in which they are legal
    :type format_map: dict

    :return: List of set codes that are legal in the specified format
    :rtype: [str]
    """
    return list(
        filter(
            lambda code: target_format in format_map[code],
            format_map.keys()
        )
    )
