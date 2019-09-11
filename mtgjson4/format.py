import json
import logging

logger = logging.getLogger(__name__)


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


class UnrecognizedFormatException(Exception):
    pass  # TODO: Define MTGJSON exception hierarchy.


def build_format_map(all_sets_path, regular=True):
    """
    For each set in the specified JSON file, determine its legal sets and return a dictionary mapping set code to
    a list of legal formats.

    :param all_sets_path: Path to AllSets.json file
    :type all_sets_path: str
    :param regular: If this is True, then only expansions, core and draft innovation sets shall be included.
    :type regular: bool

    :return: Dictionary of the form { format: [codes] }
    :rtype: dict
    """
    try:
        with open(all_sets_path, encoding='utf-8', mode='r') as all_sets_json:
            all_sets = json.load(all_sets_json)
            formats = {fmt: [] for fmt in FORMATS}

            for code, data in all_sets.items():
                if regular and data['type'] not in ['expansion', 'core', 'draft_innovation']:
                    continue

                possible_formats = set(FORMATS)
                cards = data.get('cards')

                for card in cards:
                    card_formats = set(card.get('legalities').keys())
                    possible_formats &= card_formats

                for fmt in possible_formats:
                    formats[fmt].append(code)

            return formats

    except IOError:
        logger.exception("Could not open {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.

    except json.JSONDecodeError:
        logger.exception("Could not decode JSON from {}.".format(all_sets_path))
        raise  # TODO: Handle this properly.
