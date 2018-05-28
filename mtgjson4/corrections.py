import itertools
from mtgjson4 import mtg_global
from typing import List, Dict, Union, Any

ReplacementType = Dict[str, Union[str, List[str], Any]]


def apply_corrections(match_replace_rules, cards_dictionary: List[mtg_global.CardDescription]) -> None:
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


def apply_match(replacement_rule, cards_dictionary: List[mtg_global.CardDescription]) -> None:
    keys = set(replacement_rule.keys())
    cards_to_modify = parse_match(replacement_rule['match'], cards_dictionary)
    keys.remove('match')

    if 'replace' in replacement_rule.keys():
        replace(replacement_rule['replace'], cards_to_modify)
        keys.remove('replace')

    if 'remove' in replacement_rule.keys():
        # TODO: implement
        # remove(replacement_rule['remove'], cards_to_modify)
        keys.remove('remove')

    if 'prefixNumber' in replacement_rule.keys():
        # TODO: implement
        # prefix_number(replacement_rule['prefixNumber'], cards_to_modify)
        keys.remove('prefixNumber')

    if 'fixForeignNames' in replacement_rule.keys():
        fix_foreign_names(replacement_rule['fixForeignNames'], cards_to_modify)
        keys.remove('fixForeignNames')

    if 'fixFlavorNewlines' in replacement_rule.keys() and replacement_rule['fixFlavorNewlines']:
        fix_flavor_newlines(cards_to_modify)
        keys.remove('fixFlavorNewlines')

    if 'flavorAddDash' in replacement_rule.keys() and replacement_rule['flavorAddDash']:
        # TODO: Implement
        # flavor_add_dash(cards_to_modify)
        keys.remove('flavorAddDash')

    if 'flavorAddExclamation' in replacement_rule.keys() and replacement_rule['flavorAddExclamation']:
        # TODO: Implement
        # flavor_add_exclamation(cards_to_modify)
        keys.remove('flavorAddExclamation')

    if 'incrementNumber' in replacement_rule.keys() and replacement_rule['incrementNumber']:
        # TODO: Implement
        # increment_number(cards_to_modify)
        keys.remove('incrementNumber')

    if 'removeCard' in replacement_rule.keys() and replacement_rule['removeCard']:
        # TODO: Implement
        # remove_card(cards_to_modify)
        keys.remove('removeCard')

    if keys:
        raise KeyError(keys)


def no_basic_land_watermarks(cards_dictionary):
    # TODO: Not sure what to do with this.
    pass


def replace(replacements: Dict[str, Any], cards_to_modify: List[mtg_global.CardDescription]) -> None:
    for key_name, replacement in replacements.items():
        for card in cards_to_modify:
            card[key_name] = replacement  # type: ignore


def fix_foreign_names(replacements: List[Dict[str, Any]], cards_to_modify: List[mtg_global.CardDescription]) -> None:
    for lang_replacements in replacements:
        language_name = lang_replacements['language']
        new_name = lang_replacements['name']

        for card in cards_to_modify:
            for foreign_names_field in card['foreignNames']:
                if foreign_names_field['language'] == language_name:
                    foreign_names_field['name'] = new_name


def fix_flavor_newlines(cards_to_modify: List[mtg_global.CardDescription]) -> None:
    # The javascript version had the following regex to normalize em-dashes /(\s|")-\s*([^"—-]+)\s*$/
    for card in cards_to_modify:
        flavor = card.get('flavor')
        if flavor:
            # Ensure two quotes appear before the last em-dash
            # TODO
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
