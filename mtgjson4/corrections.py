import itertools
from typing import Any, Callable, Dict, Iterable, List, Union

from mtgjson4 import mtg_global

ReplacementType = Dict[str, Union[str, List[str], Any]]


def apply_corrections(match_replace_rules: Iterable[Union[dict, str]],
                      cards_dictionary: List[mtg_global.CardDescription]) -> None:
    for replacement_rule in match_replace_rules:

        if isinstance(replacement_rule, dict):
            if replacement_rule.get('match'):
                apply_match(replacement_rule, cards_dictionary)
                continue
            elif replacement_rule.get('renumberImages'):
                # TODO: Implement
                continue
            elif replacement_rule.get('copyCard'):
                # TODO Implement
                continue
            elif replacement_rule.get('importCard'):
                # TODO: implement
                # This one sounds like it might be messy.  I'm not sure how to do it.
                continue
        elif isinstance(replacement_rule, str):
            if replacement_rule == 'noBasicLandWatermarks':
                no_basic_land_watermarks(cards_dictionary)
                continue
            elif replacement_rule == 'numberCards':
                # TODO: Implement
                continue
            elif replacement_rule == 'sortCards':
                # TODO: Implement
                continue
        raise KeyError(replacement_rule)


def apply_match(replacement_rule: dict, cards_dictionary: List[mtg_global.CardDescription]) -> None:
    keys = set(replacement_rule.keys())
    cards_to_modify = parse_match(replacement_rule['match'], cards_dictionary)
    keys.remove('match')

    rules: Dict[str, Callable] = {
        'replace': replace,
        'remove': remove,
        'prefixNumber': prefix_number,
        'fixForeignNames': fix_foreign_names,
        'fixFlavorNewlines': fix_flavor_newlines,
        'flavorAddDash': flavor_add_dash,
        'flavorAddExclamation': flavor_add_exclamation,
        'incrementNumber': increment_number,
        'removeCard': remove_card,
    }

    for action in keys:
        rules[action](replacement_rule[action], cards_to_modify)


def replace(replacements: Dict[str, Any], cards_to_modify: List[mtg_global.CardDescription]) -> None:
    """
    Replaces the values of fields to other fields.
    """
    for key_name, replacement in replacements.items():
        for card in cards_to_modify:
            card[key_name] = replacement  # type: ignore


def remove(removals: List[str], cards_to_modify: List[mtg_global.CardDescription]) -> None:
    """
    Removes the specified keys from a card.
    """
    for key_name in removals:
        for card in cards_to_modify:
            # TODO: error: "CardDescription" has no attribute "pop"
            pass
            # card.pop(key_name, None)


def prefix_number(prefix: Any, cards_to_modify: Any) -> None:
    pass


def fix_foreign_names(replacements: List[Dict[str, Any]], cards_to_modify: List[mtg_global.CardDescription]) -> None:
    """
    Sometimes the foreign names are wrong.
    This completely replaces the names with accurate ones.
    """
    for lang_replacements in replacements:
        language_name = lang_replacements['language']
        new_name = lang_replacements['name']

        for card in cards_to_modify:
            for foreign_names_field in card['foreignNames']:
                if foreign_names_field['language'] == language_name:
                    foreign_names_field['name'] = new_name


def fix_flavor_newlines(enabled: bool, cards_to_modify: List[mtg_global.CardDescription]) -> None:
    # The javascript version had the following regex to normalize em-dashes /(\s|")-\s*([^"—-]+)\s*$/
    for card in cards_to_modify:
        flavor = card.get('flavor')
        if flavor:
            # Ensure two quotes appear before the last em-dash
            # TODO
            pass


def flavor_add_dash(enabled: bool, cards_to_modify: List[mtg_global.CardDescription]) -> None:
    pass


def flavor_add_exclamation(enabled: bool, cards_to_modify: List[mtg_global.CardDescription]) -> None:
    pass


def increment_number(enabled: bool, cards_to_modify: List[mtg_global.CardDescription]) -> None:
    pass


def remove_card(enabled: bool, cards_to_modify: List[mtg_global.CardDescription]) -> None:
    pass


def parse_match(match_rule: Union[str, Dict[str, str]],
                card_list: List[mtg_global.CardDescription]) -> List[mtg_global.CardDescription]:
    if isinstance(match_rule, list):
        return itertools.chain([parse_match(rule, card_list) for rule in match_rule])
    elif isinstance(match_rule, str):
        if match_rule == "*":
            return card_list
    elif isinstance(match_rule, dict):
        matches = card_list
        for key, value in match_rule.items():
            if isinstance(value, list):
                matches = [card for card in matches if key in card.keys() and card[key] in value]
            elif isinstance(value, (int, str)):
                matches = [card for card in matches if card.get(key) == value]
        return matches
    raise KeyError(match_rule)


def no_basic_land_watermarks(cards_dictionary: Any) -> Any:
    # TODO: Not sure what to do with this.
    pass
