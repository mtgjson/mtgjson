import pytest

import mtgjson5.providers
import mtgjson5.set_builder


@pytest.mark.parametrize(
    "card_name_to_find, card_number_to_find, set_code",
    [
        ["Briarbridge Tracker", "172", "MID"],
        ["Candlelit Cavalry", "175", "MID"],
    ],
)
def test(card_name_to_find, card_number_to_find, set_code):
    """
    These cards have gone missing in the past due to upstream caching issues
    """
    scryfall_data = (
        mtgjson5.providers.scryfall.monolith.ScryfallProvider().download_cards(set_code)
    )

    scryfall_data_map_name = {card["name"]: card for card in scryfall_data}
    scryfall_data_map_number = {
        card["collector_number"]: card for card in scryfall_data
    }

    assert card_name_to_find in scryfall_data_map_name
    assert card_number_to_find in scryfall_data_map_number
