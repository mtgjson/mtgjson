import pytest

from mtgjson4.util import build_format_map

NULL_OUTPUT = {
    "standard": [],
    "pioneer": [],
    "modern": [],
    "legacy": [],
    "vintage": [],
    "pauper": [],
}


@pytest.mark.parametrize(
    "all_sets, expected",
    [
        pytest.param({}, NULL_OUTPUT),
        pytest.param(
            {
                "TS1": {"cards": [{"legalities": {"standard": "Legal"}}]},
                "TS2": {
                    "cards": [
                        {"legalities": {"standard": "Legal"}},
                        {"legalities": {"standard": "Legal", "modern": "Legal"}},
                    ]
                },
                "TS3": {
                    "cards": [
                        {"legalities": {"modern": "Legal", "standard": "Legal"}},
                        {"legalities": {"modern": "Legal"}},
                    ]
                },
                "TS4": {
                    "cards": [
                        {
                            "legalities": {
                                "vintage": "Restricted",
                                "legacy": "Legal",
                                "modern": "Legal",
                                "standard": "Legal",
                            }
                        },
                        {
                            "legalities": {
                                "vintage": "Legal",
                                "legacy": "Legal",
                                "modern": "Legal",
                                "standard": "Banned",
                            }
                        },
                    ]
                },
                "TS5": {
                    "cards": [{"legalities": {"standard": "Legal", "pioneer": "Legal"}}]
                },
            },
            {
                **NULL_OUTPUT,
                **{
                    "standard": ["TS1", "TS2", "TS4", "TS5"],
                    "pioneer": ["TS5"],
                    "modern": ["TS3", "TS4"],
                    "legacy": ["TS4"],
                    "vintage": ["TS4"],
                },
            },
        ),
    ],
)
def test_build_format_map(all_sets: dict, expected: dict) -> None:
    """
    Tests that set legality is determined correctly, given a range of inputs
    """
    result = build_format_map(all_sets, regular=False)
    assert result == expected
