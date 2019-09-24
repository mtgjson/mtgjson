import pytest

from mtgjson4.format import build_format_map


NULL_OUTPUT = {"standard": [], "modern": [], "legacy": [], "vintage": [], "pauper": []}


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
            },
            {
                **NULL_OUTPUT,
                **{
                    "standard": ["TS1", "TS2", "TS4"],
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
