import contextlib
import copy
import datetime
import hashlib
import re
from typing import Dict, List, Optional, Set, Tuple, Union

import bs4

from mtgjson4 import mtg_global
from mtgjson4.mtg_global import ColorType

PowTouLoyaltyVanType = Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]


def replace_symbol_images_with_tokens(tag: bs4.BeautifulSoup) -> List[Union[bs4.BeautifulSoup, Set[ColorType]]]:
    """
    Replaces the img tags of symbols with token representations
    :rtype: set
    :return: The color symbols found
    """
    tag_copy = copy.copy(tag)
    colors_found: Set[ColorType] = set()
    images = tag_copy.find_all('img')
    for symbol in images:
        symbol_value = symbol['alt']
        symbol_mapped = mtg_global.get_symbol_short_name(symbol_value)
        symbol.replace_with(f'{{{symbol_mapped}}}')
        if symbol_mapped in mtg_global.COLORS:
            colors_found.add(symbol_mapped)

    return [tag_copy, colors_found]


def parse_card_name(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    """
    Parse the card name from the row
    :param soup:
    :param parse_div:
    :return: card name from MID
    """
    name_row = soup.find(id=parse_div.format('nameRow'))
    name_row = name_row.findAll('div')[-1]
    card_name = str(name_row.get_text(strip=True))

    return card_name


def parse_card_cmc(soup: bs4.BeautifulSoup, parse_div: str) -> Union[int, float]:
    """
    Parse the card CMC from the row
    :param soup:
    :param parse_div:
    :return: cmc from MID
    """
    cmc_row = soup.find(id=parse_div.format('cmcRow'))

    if cmc_row is None:
        return 0

    cmc_row = cmc_row.findAll('div')[-1]
    cmc_str = cmc_row.get_text(strip=True)

    try:
        card_cmc: Union[int, float] = int(cmc_str)
    except ValueError:  # Little Girl causes this, for example
        card_cmc = float(cmc_str)

    return card_cmc


def parse_card_other_name(soup: bs4.BeautifulSoup, parse_div: str, layout: str) -> List[Union[bool, Optional[str]]]:
    """
    If the MID has 2 cards, return the other card's name
    :param soup:
    :param parse_div:
    :param layout:
    :return: If the layout matches, return the other card's name
    """
    if layout == 'double':
        if 'ctl02' in parse_div:
            other_div_name = parse_div.replace('02', '03')
        else:
            other_div_name = parse_div.replace('03', '02')

        other_name_row = soup.find(id=other_div_name.format('nameRow'))
        other_name_row = other_name_row.findAll('div')[-1]
        card_other_name = other_name_row.get_text(strip=True)

        return [True, card_other_name]

    return [False, None]


def parse_card_types(soup: bs4.BeautifulSoup, parse_div: str) -> List[Union[List[str], List[str], List[str], str]]:
    """
    Parse the types of the card and split them into 4 different structures
    super types, normal types, sub types, and the full row (all the types)
    :param soup:
    :param parse_div:
    :return:
    """
    card_super_types: List[str] = []
    card_types: List[str] = []
    card_sub_types: List[str] = []

    type_row = soup.find(id=parse_div.format('typeRow'))
    type_row = type_row.findAll('div')[-1]
    type_row = type_row.get_text(strip=True).replace('  ', ' ')

    if '—' in type_row:
        supertypes_and_types, subtypes = type_row.split('—')
        card_sub_types = subtypes.split()
    else:
        supertypes_and_types = type_row

    for value in supertypes_and_types.split():
        if value in mtg_global.SUPERTYPES:
            card_super_types.append(value)
        elif value in mtg_global.CARD_TYPES:
            card_types.append(value)
        else:
            card_types.append(value)
            # raise ValueError(f'Unknown supertype or card type: {value}')

    return [card_super_types, card_types, card_sub_types, type_row]


def parse_colors_and_cost(soup: bs4.BeautifulSoup,
                          parse_div: str) -> List[Union[Optional[List[ColorType]], Optional[str]]]:
    """
    Parse the colors and mana cost of the card
    Can use the colors to build the color identity later
    :param soup:
    :param parse_div:
    :return:
    """
    mana_row = soup.find(id=parse_div.format('manaRow'))
    if mana_row:
        mana_row = mana_row.findAll('div')[-1]
        mana_row = replace_symbol_images_with_tokens(mana_row)

        card_cost = mana_row[0].get_text(strip=True).replace('’', '\'')
        card_colors: Set[ColorType] = set(mana_row[1])

        # Sort field in WUBRG order
        sorted_colors = sorted(
            list(filter(lambda c: c in card_colors, mtg_global.COLORS)),
            key=lambda word: [mtg_global.COLORS.index(ColorType(c)) for c in word])

        return [sorted_colors, card_cost]

    return [None, None]


def parse_card_text_and_color_identity(
        soup: bs4.BeautifulSoup, parse_div: str,
        card_colors: Optional[List[ColorType]]) -> List[Union[Optional[str], List[ColorType]]]:
    text_row = soup.find(id=parse_div.format('textRow'))
    return_text = ''
    return_color_identity = set()

    if card_colors:
        return_color_identity.update(card_colors)

    if text_row is not None:
        text_row = text_row.select('div[class^=cardtextbox]')

        return_text = ''
        for div in text_row:
            # Start by replacing all images with alternative text
            div, instance_color_identity = replace_symbol_images_with_tokens(div)

            return_color_identity.update(instance_color_identity)

            # Next, just add the card text, line by line
            return_text += div.get_text() + '\n'

        return_text = return_text.strip()  # Remove last '\n'

    # Sort field in WUBRG order
    sorted_color_identity = sorted(
        list(filter(lambda c: c in return_color_identity, mtg_global.COLORS)),
        key=lambda word: [mtg_global.COLORS.index(ColorType(c)) for c in word])

    return [return_text or None, sorted_color_identity]


def parse_card_flavor(soup: bs4.BeautifulSoup, parse_div: str) -> Optional[str]:
    flavor_row = soup.find(id=parse_div.format('flavorRow'))
    card_flavor_text = ''
    if flavor_row is not None:
        flavor_row = flavor_row.select('div[class^=flavortextbox]')

        for div in flavor_row:
            card_flavor_text += div.get_text() + '\n'

        card_flavor_text = card_flavor_text.strip()  # Remove last '\n'

    if not card_flavor_text:
        return None
    return card_flavor_text


def parse_card_pt_loyalty_vanguard(soup: bs4.BeautifulSoup, parse_div: str) -> PowTouLoyaltyVanType:
    pt_row = soup.find(id=parse_div.format('ptRow'))

    power = None
    toughness = None
    loyalty = None
    hand = None
    life = None

    if pt_row is not None:
        pt_row = pt_row.findAll('div')[-1]
        pt_row = pt_row.get_text(strip=True)

        # If Vanguard
        if 'Hand Modifier' in pt_row:
            pt_row = pt_row.split('\xa0,\xa0')
            hand = pt_row[0].split(' ')[-1]
            life = pt_row[1].split(' ')[-1][:-1]
        elif '/' in pt_row:
            card_power, card_toughness = pt_row.split('/')
            power = card_power.strip()
            toughness = card_toughness.strip()
        else:
            loyalty = pt_row.strip()

    return (power, toughness, loyalty, hand, life)


def parse_card_rarity(soup: bs4.BeautifulSoup, parse_div: str) -> str:
    rarity_row = soup.find(id=parse_div.format('rarityRow'))
    rarity_row = rarity_row.findAll('div')[-1]
    card_rarity = str(rarity_row.find('span').get_text(strip=True))
    return card_rarity


def parse_card_number(soup: bs4.BeautifulSoup, parse_div: str) -> Optional[str]:
    number_row = soup.find(id=parse_div.format('numberRow'))
    card_number = None
    if number_row is not None:
        number_row = number_row.findAll('div')[-1]
        card_number = number_row.get_text(strip=True)

    return card_number


def parse_artists(soup: bs4.BeautifulSoup, parse_div: str) -> List[str]:
    with contextlib.suppress(AttributeError):  # Un-cards might not have an artist!
        artist_row = soup.find(id=parse_div.format('artistRow'))
        artist_row = artist_row.findAll('div')[-1]
        card_artists = artist_row.find('a').get_text(strip=True).split('&')

    return card_artists if card_artists else list()


def parse_watermark(soup: bs4.BeautifulSoup, parse_div: str) -> Optional[str]:
    card_watermark = None
    watermark_row = soup.find(id=parse_div.format('markRow'))
    if watermark_row is not None:
        watermark_row = watermark_row.findAll('div')[-1]
        card_watermark = watermark_row.get_text(strip=True)

    return card_watermark


def parse_rulings(soup: bs4.BeautifulSoup, parse_div: str) -> List[dict]:
    rulings: List[Dict[str, str]] = list()
    rulings_row = soup.find(id=parse_div.format('rulingsRow'))
    if rulings_row is not None:
        rulings_dates = rulings_row.findAll('td', id=re.compile(r'\w*_rulingDate\b'))
        rulings_text = rulings_row.findAll('td', id=re.compile(r'\w*_rulingText\b'))

        rulings_text = [
            replace_symbol_images_with_tokens(ruling_text)[0].get_text().replace('’', '\'')
            for ruling_text in rulings_text
        ]

        rulings_dates = [
            datetime.datetime.strptime(rulings_date.get_text(), '%m/%d/%Y').strftime('%Y-%m-%d')
            for rulings_date in rulings_dates
        ]

        rulings = [{
            'date': ruling_date,
            'text': ruling_text
        } for ruling_date, ruling_text in zip(rulings_dates, rulings_text)]

    return rulings


def parse_card_sets(soup: bs4.BeautifulSoup, parse_div: str, card_set: str,
                    sets_to_build: List[List[str]]) -> List[str]:
    card_printings = [card_set]
    sets_row = soup.find(id=parse_div.format('otherSetsRow'))
    if sets_row is not None:
        images = sets_row.findAll('img')

        for symbol in images:
            this_set_name = symbol['alt'].split('(')[0].strip()

            card_printings += (set_code[1] for set_code in sets_to_build if this_set_name == set_code[0])

    return card_printings


def parse_card_variations(soup: bs4.BeautifulSoup, parse_div: str, card_mid: int) -> List[int]:
    card_variations = []
    variations_row = soup.find(id=parse_div.format('variationLinks'))
    if variations_row is not None:
        for variations_info in variations_row.findAll('a', {'class': 'variationLink'}):
            card_variations.append(int(variations_info['href'].split('multiverseid=')[1]))

        with contextlib.suppress(ValueError):
            card_variations.remove(card_mid)  # Don't need this card's MID in its variations

    return card_variations


def parse_card_legal(soup: bs4.BeautifulSoup) -> List[dict]:
    format_rows = soup.select('table[class^=cardList]')[1]
    format_rows = format_rows.select('tr[class^=cardItem]')
    card_formats = []
    with contextlib.suppress(IndexError):  # if no legalities, only one tr with only one td
        for div in format_rows:
            table_rows = div.findAll('td')
            card_format_name = table_rows[0].get_text(strip=True)
            card_format_legal = table_rows[1].get_text(strip=True)  # raises IndexError if no legalities

            card_formats.append({'format': card_format_name, 'legality': card_format_legal})

    return card_formats


def parse_foreign_info(soup: bs4.BeautifulSoup) -> List[dict]:
    language_rows = soup.select('table[class^=cardList]')[0]
    language_rows = language_rows.select('tr[class^=cardItem]')

    card_languages = []
    for div in language_rows:
        table_rows = div.findAll('td')

        a_tag = table_rows[0].find('a')
        foreign_mid = a_tag['href'].split('=')[-1]
        card_language_mid = int(foreign_mid)
        card_foreign_name_in_language = a_tag.get_text(strip=True)

        card_language_name = table_rows[1].get_text(strip=True)

        card_languages.append({
            'language': card_language_name,
            'name': card_foreign_name_in_language,
            'multiverseid': card_language_mid
        })

    return card_languages


def build_id_part(set_name: List[str], card_mid: int, card_info: dict) -> str:
    card_hash = hashlib.sha3_256()
    card_hash.update(set_name[0].encode('utf-8'))
    card_hash.update(str(card_mid).encode('utf-8'))
    card_hash.update(card_info['name'].encode('utf-8'))

    return card_hash.hexdigest()
