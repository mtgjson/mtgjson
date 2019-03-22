from mtgjson4.__main__ import get_all_sets
from mtgjson4.provider import scryfall
from mtgjson4.provider.wizards import get_translations

if __name__ == "__main__":
    magic_sets = [scryfall.get_set_header(s)["name"] for s in get_all_sets()]
    translation_table = get_translations()

    for cs in magic_sets:
        try:
            x = translation_table[cs]
        except KeyError:
            try_single = get_translations(cs)
