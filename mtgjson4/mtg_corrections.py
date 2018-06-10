import copy
import itertools
import re
from typing import Any, Callable, Dict, Iterable, List, Union, Set

from mtgjson4 import mtg_global

ReplacementType = Dict[str, Union[str, List[str], Any]]
CardList = List[mtg_global.CardDescription]


def apply_corrections(match_replace_rules: Iterable[Union[dict, str]], cards_dictionary: CardList) -> None:
    """
    Read the inputs and determine the appropriate type of
    fix to be applied
    """
    for replacement_rule in match_replace_rules:
        if isinstance(replacement_rule, dict):
            if replacement_rule.get('match'):
                apply_match(replacement_rule, cards_dictionary)
                continue
            elif replacement_rule.get('renumberImages'):
                # We no longer set the imagename property.
                continue
            elif replacement_rule.get('copyCard'):
                copy_card(replacement_rule, cards_dictionary)
                continue
            elif replacement_rule.get('importCard'):
                add_card(replacement_rule, cards_dictionary)
                continue
        elif isinstance(replacement_rule, str):
            if replacement_rule == 'noBasicLandWatermarks':
                no_basic_land_watermarks(cards_dictionary)
                continue
            elif replacement_rule == 'numberCards':
                # This doesn't make any sense and will be discontinued
                continue
            elif replacement_rule == 'sortCards':
                # This only was applied to MCI sets, which are discontinued
                continue
        raise KeyError(replacement_rule)


def apply_match(replacement_rule: dict, full_set: CardList) -> None:
    """
    Take the replacement rules and function-pointer style
    call the methods to fix the card(s)
    """
    keys = set(replacement_rule.keys())
    cards_to_modify = parse_match(replacement_rule['match'], full_set)
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
        'removeDuplicates': remove_duplicates,
    }

    for action in keys:
        rules[action](replacement_rule[action], cards_to_modify, full_set)


def replace(replacements: Dict[str, Any], cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Replaces the values of fields to other fields.
    """
    for key_name, replacement in replacements.items():
        for card in cards_to_modify:
            card[key_name] = replacement  # type: ignore


def remove(removals: List[str], cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Removes the specified keys from a card.
    """
    for key_name in removals:
        for card in cards_to_modify:
            # We need to type: ignore because of https://github.com/python/mypy/issues/3843
            card.pop(key_name, None)  # type: ignore


def prefix_number(prefix: str, cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    If the card number needs a pre-pend, this method will accomplish that
    """
    for card in cards_to_modify:
        card['number'] = prefix + card['number']


def fix_foreign_names(replacements: List[Dict[str, Any]], cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Sometimes the foreign names are wrong.
    This completely replaces the names with accurate ones.
    """
    for lang_replacements in replacements:
        language_name = lang_replacements['language']
        new_name = lang_replacements['name']

        for card in cards_to_modify:
            for foreign_names_field in card['foreignData']:
                if foreign_names_field['language'] == language_name:
                    foreign_names_field['name'] = new_name


def fix_flavor_newlines(enabled: bool, cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    "When a card's flavortext is an attributed quote, the attribution should be on the next line"
    -Katelyn
    """
    # The javascript version had the following regex to normalize em-dashes /(\s|")-\s*([^"—-]+)\s*$/
    if not enabled:
        return
    for card in cards_to_modify:
        flavor = card.get('flavor')
        if flavor and "—" in flavor:
            # Ensure two quotes appear before the last em-dash
            firstquote = flavor.index('"')
            secondquote = flavor[firstquote + 1:].index('"')
            if firstquote and secondquote:
                card['flavor'] = re.sub(r'\s*—\s*([^—]+)\s*$', r'\n—\1', flavor)


def flavor_add_dash(enabled: bool, cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Speaking of attributed quotations, they should also have the em-dash between the quote and the speaker.
    """
    if not enabled:
        return
    for card in cards_to_modify:
        flavor = card.get('flavor')
        if flavor:
            flavor = re.sub(r"""([.!?,'])(["][/]?[\n]?)(\s*)([A-Za-z])""", r'\1\2\n—\4', flavor)
            card['flavor'] = flavor


def flavor_add_exclamation(enabled: bool, cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Gatherer is really bad at listing exclaimation points.
    """
    if not enabled:
        return
    for card in cards_to_modify:
        flavor = card.get('flavor')
        if flavor:
            card['flavor'] = re.sub(r'([A-Za-z])"', r'\1!"', flavor)


def increment_number(enabled: bool, cards_to_modify: CardList, *args: Any) -> None:
    # pylint: disable-msg=unused-argument
    """
    Fix numbers for basic lands.
    Usually preceded by a "replace: number"
    """
    # Seems like a hack to correct MCI imports?
    # I don't think we need it.
    if not enabled:
        return
    counts: Dict[str, int] = dict()
    for card in cards_to_modify:
        addition = counts.get(card['name'], 0)
        card['number'] = str(int(card['number']) + addition)
        counts[card['name']] = addition + 1


def remove_card(enabled: bool, cards_to_modify: CardList, full_set: CardList) -> None:
    """
    Completely remove the cards from the set.
    """
    if not enabled:
        return
    for card in cards_to_modify:
        full_set.remove(card)


def remove_duplicates(enabled: bool, cards_to_modify: CardList, full_set: CardList) -> None:
    """
    Sometimes a card appears twice in the set. We don't want that.
    """
    if not enabled:
        return
    hashes: Set[str] = set()
    for card in cards_to_modify:
        if card['cardHash'] in hashes:
            full_set.remove(card)
        hashes.add(card['cardHash'])


def copy_card(replacement_rule: dict, full_set: CardList) -> None:
    """
    Copy a card already in the set, calling any replacement rules necessary,
    and adding it to the set
    """
    card_name = replacement_rule['copyCard']
    replacements = replacement_rule['replace']

    for card in full_set:
        if card['name'] == card_name:
            # Copy the card, fix the card, and add the card to the set
            new_addition = copy.copy(card)
            print(replacements, new_addition)
            replace(replacements, [new_addition])
            full_set.append(new_addition)
            return


def add_card(replacement_rule: dict, full_set: CardList) -> None:
    """
    Add a new card to the set (name only)
    Should be appended later with a match/replace
    """
    card_to_add: mtg_global.CardDescription
    card_to_add['name'] = replacement_rule['importCard']['name']
    full_set.append(card_to_add)


def parse_match(match_rule: Union[str, Dict[str, str]], card_list: CardList) -> CardList:
    """
    Based on the replacement rule, we need to get all the cards necessary to
    be modified by the rule
    """
    if isinstance(match_rule, list):
        return list(itertools.chain([parse_match(rule, card_list) for rule in match_rule]))
    elif isinstance(match_rule, str):
        if match_rule == '*':
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


def no_basic_land_watermarks(card_dictionary: Any) -> Any:
    """
    Basic lands in most sets have watermarks.
    This function removes that field for those
    sets which don't conform
    """
    for card in card_dictionary:
        if card['name'] in mtg_global.basic_lands:
            card.pop('watermark', None)
