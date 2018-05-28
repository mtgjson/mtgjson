import bs4
import copy
from typing import List, Set, Union

from mtgjson4.globals import COLORS, Color, get_symbol_short_name


def replace_symbol_images_with_tokens(
        tag: bs4.BeautifulSoup) -> List[Union[bs4.BeautifulSoup, Set[Color]]]:
    """
    Replaces the img tags of symbols with token representations
    :rtype: set
    :return: The color symbols found
    """
    tag_copy = copy.copy(tag)
    colors_found: Set[Color] = set()
    images = tag_copy.find_all('img')
    for symbol in images:
        symbol_value = symbol['alt']
        symbol_mapped = get_symbol_short_name(symbol_value)
        symbol.replace_with(f'{{{symbol_mapped}}}')
        if symbol_mapped in COLORS:
            colors_found.add(symbol_mapped)

    return [tag_copy, colors_found]
