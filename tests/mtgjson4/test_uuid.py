from typing import Dict, Any

import pytest

from mtgjson4.compile_mtg import get_uuid, get_uuid_421


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
    uuid_new = get_uuid(mock_card)
    uuid_old = get_uuid_421(mock_card, mock_file_info)

    assert uuid_old == "3d5e8a9a-d922-5abd-86bc-04ad363641dd"
    assert uuid_new == "4b560297-2f1e-5f65-b118-289c21bdf887"
