from typing import Any, Dict

from mtgjson4.mtgjson_card import MTGJSONCard
import pytest


@pytest.mark.parametrize(
    "mock_card, mock_file_info",
    [
        (
            {
                "colors": ["W"],
                "name": "Gisela, the Broken Blade",
                "names": [
                    "Bruna, the Fading Light",
                    "Brisela, Voice of Nightmares",
                    "Gisela, the Broken Blade",
                ],
                "originalText": "Flying, first strike, lifelink\nAt the beginning of your end step, if you both own and "
                + "control Gisela, the Broken Blade and a creature named Bruna, the Fading Light, "
                + "exile them, then meld them into Brisela, Voice of Nightmares.",
                "originalType": "Legendary Creature — Angel Horror",
                "scryfallId": "c75c035a-7da9-4b36-982d-fca8220b1797",
                "side": "b",
            },
            {"code": "EMN"},
        )
    ],
)
def test_uuid_creation(
    mock_card: Dict[str, Any], mock_file_info: Dict[str, Any]
) -> None:
    """
    Tests to ensure UUIDs don't regress
    :param mock_card:
    :param mock_file_info:
    :return:
    """
    card = MTGJSONCard(mock_file_info["code"])
    card.set_all(mock_card)

    uuid_new = card.get_uuid()
    assert uuid_new == "4b560297-2f1e-5f65-b118-289c21bdf887"
