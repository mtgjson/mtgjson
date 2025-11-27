"""Pytest configuration and fixtures for MTGJSON tests."""

import os
from typing import Any, Dict, Generator, Optional

import pytest
import requests_cache

from mtgjson5.mtgjson_config import MtgjsonConfig


@pytest.fixture
def disable_cache() -> Generator[None, None, None]:
    """
    Disable requests-cache for VCR tests.

    Uses requests_cache.disabled() context manager (the official API) rather than
    monkey-patching CachedSession. This approach:
    - Is documented in requests-cache compatibility guide
    - Properly handles global cache state
    - Won't break if library internals change
    - Makes the intent explicit

    Alternative (NOT used): unittest.mock.patch('requests_cache.CachedSession', requests.Session)
    - More brittle, depends on internal implementation details
    - Doesn't handle all caching mechanisms (e.g., globally installed cache)

    While requests-cache and VCR can coexist, disabling the cache during VCR tests
    ensures deterministic playback from cassettes without cache interference.

    See: https://requests-cache.readthedocs.io/en/stable/user_guide/compatibility.html
    """
    with requests_cache.disabled():
        yield


# VCR configuration
@pytest.fixture(scope="module")
def vcr_config() -> Dict[str, Any]:
    """
    Configure VCR for deterministic HTTP testing.

    Filters volatile headers and sets decode mode for reproducible cassettes.
    TCGplayer-specific: masks OAuth tokens and credentials in cassettes.
    """
    import json

    def scrub_tcgplayer_oauth_response(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scrub sensitive fields from TCGplayer OAuth responses.

        Redacts:
        - access_token: Bearer token for API access
        - userName: User identifier that may contain sensitive info

        This ensures recorded cassettes don't contain actual credentials.
        """
        try:
            body = response["body"]["string"]
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            data = json.loads(body)
        except Exception:
            # If we can't parse JSON, return response unchanged
            return response

        if isinstance(data, dict):
            if "access_token" in data:
                data["access_token"] = "REDACTED"
            if "userName" in data:
                data["userName"] = "REDACTED"
            response["body"]["string"] = json.dumps(data).encode("utf-8")

        return response

    return {
        # Remove headers that change between requests to ensure cassette stability
        # Without this, cassettes would be invalidated on every API change
        "filter_headers": [
            "authorization",  # API keys/tokens (security + stability)
            ("Authorization", "Bearer REDACTED"),  # TCGplayer: redact bearer tokens
            "date",  # Server timestamp (changes every request)
            "server",  # Server version info (changes with deployments)
            "cf-cache-status",  # Cloudflare cache status (non-deterministic)
            "expires",  # Cache expiry time (time-dependent)
            "etag",  # Resource version identifier (changes with updates)
            "last-modified",  # Resource modification time (time-dependent)
        ],
        # TCGplayer OAuth: filter sensitive POST form data
        "filter_post_data_parameters": [
            "grant_type",
            "client_id",
            "client_secret",
        ],
        # Automatically decode gzip/deflate responses for human-readable cassettes
        # Without this, cassette YAML would contain binary compressed data
        "decode_compressed_response": True,
        # TCGplayer OAuth: scrub access_token and userName from response bodies
        "before_record_response": scrub_tcgplayer_oauth_response,
        # Match requests by method, scheme, host, port, path, and query
        # This ensures stable cassette matching for TCGplayer token requests
        "match_on": ["method", "scheme", "host", "port", "path", "query"],
        # Default record mode: "once" for local dev, "none" for offline/CI testing
        # Can be overridden with --record-mode flag
        # See README "Testing with VCR Cassettes" for mode explanations
        "record_mode": "none" if os.environ.get("MTGJSON_OFFLINE_MODE") else "once",
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request: pytest.FixtureRequest) -> str:
    """Set cassette directory for VCR using absolute path.

    Overrides pytest-recording's default behavior which places cassettes
    relative to each test module. Instead, all cassettes go in tests/cassettes/
    to match the project structure where cassettes are organized by provider
    and test name (e.g., providers/<provider>/<test_name>.yml).
    """
    # Get project root from pytest config
    root = request.config.rootpath
    return os.path.join(str(root), "tests", "cassettes")


@pytest.fixture
def tcgplayer_config() -> Generator[Any, None, None]:
    """
    Fixture to temporarily modify TCGPlayer config for testing.

    Provides a helper function to set/remove [TCGPlayer] section and options
    in the MtgjsonConfig singleton without cross-test leakage.

    Usage:
        def test_something(tcgplayer_config):
            tcgplayer_config(client_id="test_id", client_secret="test_secret")
            # Test code that uses MtgjsonConfig

        def test_missing_config(tcgplayer_config):
            tcgplayer_config(present=False)  # Remove entire [TCGPlayer] section
            # Test code that expects missing config
    """
    cfg = MtgjsonConfig()
    parser = cfg.config_parser

    # Snapshot current state for restoration
    snapshot_defaults = dict(parser.defaults())
    snapshot = {s: dict(parser.items(s)) for s in parser.sections()}

    def set_tcgplayer_config(
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        api_version: Optional[str] = None,
        present: bool = True,
    ) -> None:
        """
        Set or remove TCGPlayer configuration.

        SECURITY: Clears ALL config sections first to prevent other API keys
        from being captured in VCR cassettes during recording.

        Args:
            client_id: OAuth client ID (if provided)
            client_secret: OAuth client secret (if provided)
            api_version: API version string (if provided)
            present: If False, remove entire [TCGPlayer] section
        """
        # IMPORTANT: Clear ALL sections to prevent key leakage in cassettes
        # This ensures only test credentials are present during recording
        for section in list(parser.sections()):
            parser.remove_section(section)

        section = "TCGPlayer"

        if not present:
            # Already cleared above, nothing more to do
            return

        parser.add_section(section)

        # Only set keys that are explicitly provided
        # Omission simulates missing keys in config
        if client_id is not None:
            parser.set(section, "client_id", client_id)

        if client_secret is not None:
            parser.set(section, "client_secret", client_secret)

        if api_version is not None:
            parser.set(section, "api_version", api_version)

    try:
        yield set_tcgplayer_config
    finally:
        # Restore original state
        parser.clear()
        if snapshot_defaults:
            parser["DEFAULT"] = snapshot_defaults
        for sec, opts in snapshot.items():
            if not parser.has_section(sec):
                parser.add_section(sec)
            for k, v in opts.items():
                parser.set(sec, k, v)


# Fixtures for set_builder tests
@pytest.fixture
def sample_scryfall_card_normal() -> Dict[str, Any]:
    """
    Sample normal Scryfall card for testing.

    Represents a simple creature card with standard attributes.
    """
    return {
        "object": "card",
        "id": "5f519952-0b8a-4a3e-8f3d-ecf26faef8cc",
        "oracle_id": "ef1a8e9f-c0d1-4c3c-9e3b-3d0e8c8f1e8f",
        "name": "Grizzly Bears",
        "lang": "en",
        "released_at": "2020-07-01",
        "layout": "normal",
        "mana_cost": "{1}{G}",
        "cmc": 2.0,
        "type_line": "Creature — Bear",
        "oracle_text": "",
        "colors": ["G"],
        "color_identity": ["G"],
        "keywords": [],
        "legalities": {
            "standard": "legal",
            "modern": "legal",
            "legacy": "legal",
            "vintage": "legal",
        },
        "games": ["paper", "mtgo"],
        "reserved": False,
        "foil": True,
        "nonfoil": True,
        "finishes": ["nonfoil", "foil"],
        "oversized": False,
        "promo": False,
        "reprint": True,
        "variation": False,
        "set": "m21",
        "set_name": "Core Set 2021",
        "set_type": "core",
        "collector_number": "200",
        "digital": False,
        "rarity": "common",
        "flavor_text": "Don't try to outrun one of Dominia's Grizzlies.",
        "artist": "Jeff A. Menges",
        "artist_ids": ["8efa2cfd-5e18-4f0c-ae66-e7add3e8f7a9"],
        "illustration_id": "99c56c3e-a77e-4e3a-a3a9-8e3a3a3a3a3a",
        "border_color": "black",
        "frame": "2015",
        "full_art": False,
        "textless": False,
        "booster": True,
        "story_spotlight": False,
        "edhrec_rank": 12345,
        "prices": {"usd": "0.25", "usd_foil": "0.50"},
        "related_uris": {},
        "purchase_uris": {},
        "multiverse_ids": [123456],
        "prints_search_uri": "https://api.scryfall.com/cards/search?q=oracleid:ef1a8e9f",
        "rulings_uri": "https://api.scryfall.com/cards/5f519952/rulings",
        "power": "2",
        "toughness": "2",
    }


@pytest.fixture
def sample_scryfall_card_dfc() -> Dict[str, Any]:
    """
    Sample double-faced card (DFC) for testing.

    Represents a transform card with two faces.
    """
    return {
        "object": "card",
        "id": "7e46788a-b8a4-4a3e-8f3d-ecf26faef8dd",
        "oracle_id": "ab2c3d4e-5f6g-7h8i-9j0k-1l2m3n4o5p6q",
        "name": "Delver of Secrets // Insectile Aberration",
        "lang": "en",
        "released_at": "2011-09-30",
        "layout": "transform",
        "mana_cost": "{U}",
        "cmc": 1.0,
        "type_line": "Creature — Human Wizard // Creature — Human Insect",
        "colors": ["U"],
        "color_identity": ["U"],
        "keywords": ["Flying"],
        "card_faces": [
            {
                "object": "card_face",
                "name": "Delver of Secrets",
                "mana_cost": "{U}",
                "type_line": "Creature — Human Wizard",
                "oracle_id": "ab2c3d4e-5f6g-7h8i-9j0k-1l2m3n4o5p6q",
                "oracle_text": "At the beginning of your upkeep, look at the top card of your library. You may reveal that card. If an instant or sorcery card is revealed this way, transform Delver of Secrets.",
                "colors": ["U"],
                "power": "1",
                "toughness": "1",
                "artist": "Nils Hamm",
                "artist_ids": ["d9d5c1e0-7e3a-4f9c-b8f3-8c0f1e2f3c4d"],
                "illustration_id": "11111111-1111-1111-1111-111111111111",
            },
            {
                "object": "card_face",
                "name": "Insectile Aberration",
                "mana_cost": "",
                "type_line": "Creature — Human Insect",
                "oracle_text": "Flying",
                "colors": ["U"],
                "power": "3",
                "toughness": "2",
                "artist": "Nils Hamm",
                "artist_ids": ["d9d5c1e0-7e3a-4f9c-b8f3-8c0f1e2f3c4d"],
                "illustration_id": "22222222-2222-2222-2222-222222222222",
            },
        ],
        "legalities": {
            "standard": "not_legal",
            "modern": "legal",
            "legacy": "legal",
            "vintage": "legal",
        },
        "games": ["paper", "mtgo"],
        "reserved": False,
        "foil": True,
        "nonfoil": True,
        "finishes": ["nonfoil", "foil"],
        "oversized": False,
        "promo": False,
        "reprint": True,
        "set": "isd",
        "set_name": "Innistrad",
        "set_type": "expansion",
        "collector_number": "51",
        "digital": False,
        "rarity": "uncommon",
        "artist": "Nils Hamm",
        "artist_ids": ["d9d5c1e0-7e3a-4f9c-b8f3-8c0f1e2f3c4d"],
        "border_color": "black",
        "frame": "2003",
        "full_art": False,
        "textless": False,
        "booster": True,
        "story_spotlight": False,
        "multiverse_ids": [226749, 226755],
        "prints_search_uri": "https://api.scryfall.com/cards/search?q=oracleid:ab2c3d4e",
        "rulings_uri": "https://api.scryfall.com/cards/7e46788a/rulings",
    }


@pytest.fixture
def sample_scryfall_card_split() -> Dict[str, Any]:
    """
    Sample split card for testing.

    Represents a split card with two halves.
    """
    return {
        "object": "card",
        "id": "3b3e4f5g-6h7i-8j9k-0l1m-2n3o4p5q6r7s",
        "oracle_id": "cd3e4f5g-6h7i-8j9k-0l1m-2n3o4p5q6r7s",
        "name": "Fire // Ice",
        "lang": "en",
        "released_at": "2001-11-12",
        "layout": "split",
        "mana_cost": "{1}{R} // {1}{U}",
        "cmc": 4.0,
        "type_line": "Instant // Instant",
        "colors": ["U", "R"],
        "color_identity": ["U", "R"],
        "keywords": [],
        "card_faces": [
            {
                "object": "card_face",
                "name": "Fire",
                "mana_cost": "{1}{R}",
                "type_line": "Instant",
                "oracle_id": "cd3e4f5g-6h7i-8j9k-0l1m-2n3o4p5q6r7s",
                "oracle_text": "Fire deals 2 damage divided as you choose among one or two targets.",
                "colors": ["R"],
                "artist": "Franz Vohwinkel",
                "artist_ids": ["e1f2g3h4-i5j6-k7l8-m9n0-o1p2q3r4s5t6"],
                "illustration_id": "33333333-3333-3333-3333-333333333333",
            },
            {
                "object": "card_face",
                "name": "Ice",
                "mana_cost": "{1}{U}",
                "type_line": "Instant",
                "oracle_text": "Tap target permanent.\nDraw a card.",
                "colors": ["U"],
                "artist": "Franz Vohwinkel",
                "artist_ids": ["e1f2g3h4-i5j6-k7l8-m9n0-o1p2q3r4s5t6"],
                "illustration_id": "44444444-4444-4444-4444-444444444444",
            },
        ],
        "legalities": {
            "standard": "not_legal",
            "modern": "legal",
            "legacy": "legal",
            "vintage": "legal",
        },
        "games": ["paper", "mtgo"],
        "reserved": False,
        "foil": False,
        "nonfoil": True,
        "finishes": ["nonfoil"],
        "oversized": False,
        "promo": False,
        "reprint": False,
        "set": "apc",
        "set_name": "Apocalypse",
        "set_type": "expansion",
        "collector_number": "128",
        "digital": False,
        "rarity": "uncommon",
        "artist": "Franz Vohwinkel",
        "artist_ids": ["e1f2g3h4-i5j6-k7l8-m9n0-o1p2q3r4s5t6"],
        "border_color": "black",
        "frame": "1997",
        "full_art": False,
        "textless": False,
        "booster": True,
        "story_spotlight": False,
        "multiverse_ids": [27165],
        "prints_search_uri": "https://api.scryfall.com/cards/search?q=oracleid:cd3e4f5g",
        "rulings_uri": "https://api.scryfall.com/cards/3b3e4f5g/rulings",
    }


@pytest.fixture
def sample_scryfall_card_meld() -> Dict[str, Any]:
    """
    Sample meld card for testing.

    Represents a meld card that combines with another card.
    """
    return {
        "object": "card",
        "id": "4c4d5e6f-7g8h-9i0j-1k2l-3m4n5o6p7q8r",
        "oracle_id": "de4f5g6h-7i8j-9k0l-1m2n-3o4p5q6r7s8t",
        "name": "Midnight Scavengers // Graf Rats",
        "lang": "en",
        "released_at": "2016-07-22",
        "layout": "meld",
        "mana_cost": "{3}{B}",
        "cmc": 4.0,
        "type_line": "Creature — Human Rogue",
        "oracle_text": "When Midnight Scavengers enters the battlefield, you may return target creature card from your graveyard to your hand.\n(Melds with Graf Rats.)",
        "colors": ["B"],
        "color_identity": ["B"],
        "keywords": [],
        "power": "3",
        "toughness": "3",
        "all_parts": [
            {
                "object": "related_card",
                "id": "4c4d5e6f-7g8h-9i0j-1k2l-3m4n5o6p7q8r",
                "component": "meld_part",
                "name": "Midnight Scavengers",
                "type_line": "Creature — Human Rogue",
                "uri": "https://api.scryfall.com/cards/4c4d5e6f",
            },
            {
                "object": "related_card",
                "id": "5d5e6f7g-8h9i-0j1k-2l3m-4n5o6p7q8r9s",
                "component": "meld_part",
                "name": "Graf Rats",
                "type_line": "Creature — Rat",
                "uri": "https://api.scryfall.com/cards/5d5e6f7g",
            },
            {
                "object": "related_card",
                "id": "6e6f7g8h-9i0j-1k2l-3m4n-5o6p7q8r9s0t",
                "component": "meld_result",
                "name": "Chittering Host",
                "type_line": "Creature — Eldrazi Horror",
                "uri": "https://api.scryfall.com/cards/6e6f7g8h",
            },
        ],
        "legalities": {
            "standard": "not_legal",
            "modern": "legal",
            "legacy": "legal",
            "vintage": "legal",
        },
        "games": ["paper", "mtgo"],
        "reserved": False,
        "foil": True,
        "nonfoil": True,
        "finishes": ["nonfoil", "foil"],
        "oversized": False,
        "promo": False,
        "reprint": False,
        "set": "emn",
        "set_name": "Eldritch Moon",
        "set_type": "expansion",
        "collector_number": "96",
        "digital": False,
        "rarity": "uncommon",
        "artist": "Daarken",
        "artist_ids": ["f1g2h3i4-j5k6-l7m8-n9o0-p1q2r3s4t5u6"],
        "illustration_id": "55555555-5555-5555-5555-555555555555",
        "border_color": "black",
        "frame": "2015",
        "full_art": False,
        "textless": False,
        "booster": True,
        "story_spotlight": False,
        "multiverse_ids": [414388],
        "prints_search_uri": "https://api.scryfall.com/cards/search?q=oracleid:de4f5g6h",
        "rulings_uri": "https://api.scryfall.com/cards/4c4d5e6f/rulings",
    }
