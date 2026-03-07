"""Tests for OrientationDetector HTML parsing helpers."""

from __future__ import annotations

import bs4

from mtgjson5.providers.scryfall.orientation import OrientationDetector

# ---------------------------------------------------------------------------
# TestParseOrientation
# ---------------------------------------------------------------------------


class TestParseOrientation:
    def test_extracts_id_from_anchor(self):
        html = '<span class="card-grid-header-content"><a id="landscape">Landscape</a></span>'
        tag = bs4.BeautifulSoup(html, "html.parser").find("span")
        assert OrientationDetector._parse_orientation(tag) == "landscape"

    def test_extracts_portrait(self):
        html = '<span class="card-grid-header-content"><a id="portrait">Portrait</a></span>'
        tag = bs4.BeautifulSoup(html, "html.parser").find("span")
        assert OrientationDetector._parse_orientation(tag) == "portrait"

    def test_no_anchor_returns_none_string(self):
        html = '<span class="card-grid-header-content">No link here</span>'
        tag = bs4.BeautifulSoup(html, "html.parser").find("span")
        assert OrientationDetector._parse_orientation(tag) == "None"

    def test_anchor_without_id(self):
        html = '<span class="card-grid-header-content"><a href="#">No ID</a></span>'
        tag = bs4.BeautifulSoup(html, "html.parser").find("span")
        assert OrientationDetector._parse_orientation(tag) == "None"


# ---------------------------------------------------------------------------
# TestParseCardEntries
# ---------------------------------------------------------------------------


class TestParseCardEntries:
    def test_extracts_card_ids(self):
        html = """
        <div class="card-grid-inner">
            <div class="card-grid-item" data-card-id="uuid-001"></div>
            <div class="card-grid-item" data-card-id="uuid-002"></div>
            <div class="card-grid-item" data-card-id="uuid-003"></div>
        </div>
        """
        tag = bs4.BeautifulSoup(html, "html.parser").find("div", class_="card-grid-inner")
        result = OrientationDetector._parse_card_entries(tag)
        assert result == ["uuid-001", "uuid-002", "uuid-003"]

    def test_empty_grid(self):
        html = '<div class="card-grid-inner"></div>'
        tag = bs4.BeautifulSoup(html, "html.parser").find("div", class_="card-grid-inner")
        assert OrientationDetector._parse_card_entries(tag) == []

    def test_single_card(self):
        html = """
        <div class="card-grid-inner">
            <div class="card-grid-item" data-card-id="abc-123"></div>
        </div>
        """
        tag = bs4.BeautifulSoup(html, "html.parser").find("div", class_="card-grid-inner")
        assert OrientationDetector._parse_card_entries(tag) == ["abc-123"]

    def test_nested_content_ignored(self):
        html = """
        <div class="card-grid-inner">
            <div class="card-grid-item" data-card-id="uuid-001">
                <img src="card.jpg" />
                <span>Card Name</span>
            </div>
        </div>
        """
        tag = bs4.BeautifulSoup(html, "html.parser").find("div", class_="card-grid-inner")
        assert OrientationDetector._parse_card_entries(tag) == ["uuid-001"]
