from unittest import mock

import pytest

from mtgjson4.provider.scryfall import parse_foreign


@mock.patch("mtgjson4.provider.scryfall.download")
def test_issue_241(mocked_download):
    """test for issue #241 on GitHub"""
    mocked_download.return_value = {
        "object": "list",
        "data": [
            {
                "set": "C18",
                "multiverse_ids": [1],
                "collector_number": "1",
                "lang": "it",
            },
            {
                "set": "C18",
                "multiverse_ids": [1],
                "collector_number": "2",
                "lang": "it",
            },
            {
                "set": "C18",
                "multiverse_ids": [1],
                "collector_number": "3",
                "lang": "it",
            },
        ],
    }
    cards = parse_foreign("fake.url?some=val&unique=prints", "Plains", "1", "C18")
    assert len(cards) == 1
    expected_result = {
        "language": "Italian",
        "multiverseId": 1,
        "name": None,
        "text": None,
        "flavorText": None,
        "type": None,
    }
    assert expected_result in cards
