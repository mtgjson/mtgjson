"""Tests for FormatFilter.is_legal."""

from __future__ import annotations

import pytest

from mtgjson5.models.files import FormatFilter


class TestFormatFilter:
    @pytest.mark.parametrize(
        ("legalities", "format_name", "expected"),
        [
            ({"standard": "Legal"}, "standard", True),
            ({"vintage": "Restricted"}, "vintage", True),
            ({"modern": "Banned"}, "modern", False),
            ({}, "standard", False),
            ({"standard": "Legal"}, "pioneer", False),
            ({"standard": "Legal", "modern": "Legal"}, "modern", True),
            # Edge cases
            (None, "standard", False),
            ({"standard": "Legal"}, "", False),
            ({"standard": "NotAStatus"}, "standard", False),
        ],
        ids=[
            "legal",
            "restricted",
            "banned",
            "empty_legalities",
            "wrong_format",
            "multi_format",
            "none_legalities",
            "empty_format_name",
            "unknown_status",
        ],
    )
    def test_is_legal_dict(self, legalities: dict, format_name: str, expected: bool):
        card = {"legalities": legalities}
        assert FormatFilter.is_legal(card, format_name) is expected
