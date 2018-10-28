"""Tests for mtgjson4.provider.gatherer."""

from typing import List

import pytest

from mtgjson4.provider import gatherer


@pytest.mark.parametrize(
    "html, expected",
    [
        pytest.param(
            """
        <html>
        <table>
        <tr><td class="rightCol">
            <div class="row">
                <div class="label">
                    Card Name:
                </div>
                <div class="value">
                    One Face
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Types:
                </div>
                <div class="value">
                    Card — Single
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Card Text:
                </div>
                <div class="value">
                    <div class="cardtextbox">I have</div>
                    <div class="cardtextbox">one card</div>
                    <div class="cardtextbox">face.</div>
                </div>
            </div>
        </td></tr>
        </table>
        </html>
        """,
            [
                gatherer.GathererCard(
                    card_name="One Face",
                    original_types="Card — Single",
                    original_text="I have\none card\nface.",
                    flavor_text=None,
                )
            ],
            id="normal layout",
        ),
        pytest.param(
            """
        <html>
        <table>
        <tr><td class="rightCol">
            <div class="row">
                <div class="label">
                    Card Name:
                </div>
                <div class="value">
                    Face One
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Types:
                </div>
                <div class="value">
                    Side — One
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Flavor Text:
                </div>
                <div class="value">
                    <div class="flavortextbox">First</div>
                    <div class="flavortextbox">face.</div>
                </div>
            </div>
        </td></tr>
        <tr><td class="rightCol">
            <div class="row">
                <div class="label">
                    Card Name:
                </div>
                <div class="value">
                    Face Two
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Types:
                </div>
                <div class="value">
                    Side — Two
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Card Text:
                </div>
                <div class="value">
                    <div class="cardtextbox">Rules</div>
                    <div class="cardtextbox">two.</div>
                </div>
            </div>
        </td></tr>
        </table>
        </html>
        """,
            [
                gatherer.GathererCard(
                    card_name="Face One",
                    original_types="Side — One",
                    original_text=None,
                    flavor_text="First\nface.",
                ),
                gatherer.GathererCard(
                    card_name="Face Two",
                    original_types="Side — Two",
                    original_text="Rules\ntwo.",
                    flavor_text=None,
                ),
            ],
            id="dfc layout",
        ),
        pytest.param(
            """
        <html>
        <table>
        <tr><td class="rightCol">
            <div class="row">
                <div class="label">
                    Card Name:
                </div>
                <div class="value">
                    Symbol User
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Types:
                </div>
                <div class="value">
                    Card — Fancy
                </div>
            </div>
            <div class="row">
                <div class="label">
                    Card Text:
                </div>
                <div class="value">
                    <div class="cardtextbox"><img alt="Tap">: Add <img alt="Green">.</div>
                    <div class="cardtextbox"></div>
                </div>
            </div>
        </td></tr>
        </table>
        </html>
        """,
            [
                gatherer.GathererCard(
                    card_name="Symbol User",
                    original_types="Card — Fancy",
                    original_text="{T}: Add {G}.",
                    flavor_text=None,
                )
            ],
            id="symbols",
        ),
    ],
)
def test_parse_cards(html: str, expected: List[gatherer.GathererCard]) -> None:
    assert gatherer.parse_cards(html) == expected
