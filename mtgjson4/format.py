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


def build_format_map(all_sets, regular=True):
    """
    For each set in the specified JSON file, determine its legal sets and return a dictionary mapping set code to
    a list of legal formats.

    :param all_sets: AllSets content
    :type all_sets: dict
    :param regular: If this is True, then unusual sets will be excluded.
    :type regular: bool

    :return: Dictionary of the form { format: [codes] }
    :rtype: dict
    """
    formats = {fmt: [] for fmt in FORMATS}

    for code, data in all_sets.items():
        if regular and data['type'] not in ['expansion', 'core', 'draft_innovation', 'commander', 'masters']:
            continue

        possible_formats = set(FORMATS)
        cards = data.get('cards')

        for card in cards:
            card_formats = set(card.get('legalities').keys())
            possible_formats &= card_formats

        for fmt in possible_formats:
            formats[fmt].append(code)

    return formats
