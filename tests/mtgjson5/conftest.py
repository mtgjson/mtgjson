"""Shared fixtures and helpers for pipeline and assembly tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

if TYPE_CHECKING:
    from mtgjson5.build.context import AssemblyContext


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Re-generate golden test files instead of asserting",
    )


@pytest.fixture(scope="session")
def update_golden(request: pytest.FixtureRequest) -> bool:
    return bool(request.config.getoption("--update-golden"))


def make_face_struct(**overrides: Any) -> dict[str, Any]:
    """Build a dict matching CardFace.polars_schema() struct fields."""
    defaults: dict[str, Any] = {
        "object": "card_face",
        "name": "Test Face",
        "mana_cost": "{1}{W}",
        "type_line": "Creature — Human",
        "oracle_text": "Test text",
        "colors": ["W"],
        "color_indicator": None,
        "power": "2",
        "toughness": "2",
        "defense": None,
        "loyalty": None,
        "flavor_text": None,
        "flavor_name": None,
        "illustration_id": None,
        "image_uris": None,
        "artist": "Test Artist",
        "artist_id": None,
        "watermark": None,
        "printed_name": None,
        "printed_text": None,
        "printed_type_line": None,
        "cmc": None,
        "oracle_id": None,
        "layout": None,
    }
    defaults.update(overrides)
    return defaults


def make_card_row(**overrides: Any) -> dict[str, Any]:
    """Return a dict with sensible defaults for every column the pipeline expects.

    Caller overrides only fields relevant to their test.
    """
    defaults: dict[str, Any] = {
        "name": "Test Card",
        "setCode": "TST",
        "number": "1",
        "layout": "normal",
        "uuid": "test-uuid-001",
        "scryfallId": "sf-001",
        "side": None,
        "faceId": 0,
        "manaCost": "{1}{W}",
        "type": "Creature — Human Warrior",
        "text": "Test card text.",
        "colors": ["W"],
        "colorIdentity": ["W"],
        "finishes": ["nonfoil"],
        "rarity": "common",
        "lang": "en",
        "language": "en",
        "manaValue": 2.0,
        "promoTypes": None,
        "frameEffects": None,
        "keywords": None,
        "power": None,
        "toughness": None,
        "boosterTypes": None,
        "cardParts": None,
        "faceName": None,
        "otherFaceIds": None,
        "_face_data": None,
        "_row_id": 0,
    }
    defaults.update(overrides)
    return defaults


def make_meld_triplet_lf(
    set_code: str = "BRO",
    lang: str = "en",
) -> pl.LazyFrame:
    """Build a 3-card LazyFrame with correct meld cardParts/faceName/number columns."""
    card_parts = ["Urza, Lord Protector", "The Mightstone and Weakstone", "Urza, Planeswalker"]
    rows = [
        {
            "setCode": set_code,
            "language": lang,
            "number": "225",
            "faceName": "Urza, Lord Protector",
            "uuid": "uuid-front1",
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
        {
            "setCode": set_code,
            "language": lang,
            "number": "238a",
            "faceName": "The Mightstone and Weakstone",
            "uuid": "uuid-front2",
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
        {
            "setCode": set_code,
            "language": lang,
            "number": "238b",
            "faceName": "Urza, Planeswalker",
            "uuid": "uuid-result",
            "cardParts": card_parts,
            "otherFaceIds": None,
        },
    ]
    return pl.LazyFrame(
        rows,
        schema={
            "setCode": pl.String,
            "language": pl.String,
            "number": pl.String,
            "faceName": pl.String,
            "uuid": pl.String,
            "cardParts": pl.List(pl.String),
            "otherFaceIds": pl.List(pl.String),
        },
    )


def make_card_lf(rows: list[dict[str, Any]] | None = None) -> pl.LazyFrame:
    """Wraps make_card_row defaults + explicit schema -> pl.LazyFrame."""
    if rows is None:
        rows = [make_card_row()]
    from mtgjson5.models.scryfall.models import CardFace

    face_struct_schema = CardFace.polars_schema()
    full_rows = [make_card_row(**r) for r in rows]
    return pl.LazyFrame(
        full_rows,
        schema={
            "name": pl.String,
            "setCode": pl.String,
            "number": pl.String,
            "layout": pl.String,
            "uuid": pl.String,
            "scryfallId": pl.String,
            "side": pl.String,
            "faceId": pl.Int64,
            "manaCost": pl.String,
            "type": pl.String,
            "text": pl.String,
            "colors": pl.List(pl.String),
            "colorIdentity": pl.List(pl.String),
            "finishes": pl.List(pl.String),
            "rarity": pl.String,
            "lang": pl.String,
            "manaValue": pl.Float64,
            "_face_data": face_struct_schema,
            "_row_id": pl.UInt32,
        },
    )


def make_full_card_df() -> pl.DataFrame:
    """Return a DataFrame with representative column types for cross-format tests.

    Includes: String, Int64, Float64, Boolean, List(String), Struct, and a null row.
    """
    return pl.DataFrame(
        {
            "uuid": ["card-001", "card-002", None],
            "name": ["Alpha Card", "Beta Card", None],
            "cmc": [3, 0, None],
            "price": [1.99, 0.0, None],
            "hasFoil": [True, False, None],
            "colors": [["W", "U"], [], None],
            "identifiers": [
                {"scryfallId": "sf-001", "multiverseId": "123"},
                {"scryfallId": "sf-002", "multiverseId": None},
                None,
            ],
        },
        schema={
            "uuid": pl.String,
            "name": pl.String,
            "cmc": pl.Int64,
            "price": pl.Float64,
            "hasFoil": pl.Boolean,
            "colors": pl.List(pl.String),
            "identifiers": pl.Struct({"scryfallId": pl.String, "multiverseId": pl.String}),
        },
    )


@pytest.fixture
def simple_ctx():  # -> PipelineContext
    """PipelineContext.for_testing() with empty meld_triplets and manual_overrides."""
    from mtgjson5.data.context import PipelineContext

    return PipelineContext.for_testing(
        meld_triplets={},
        manual_overrides={},
    )


@pytest.fixture
def meld_ctx():  # -> PipelineContext
    """PipelineContext.for_testing() with a sample meld triplet."""
    from mtgjson5.data.context import PipelineContext

    return PipelineContext.for_testing(
        meld_triplets={
            "Urza": [
                "Urza, Lord Protector",
                "The Mightstone and Weakstone",
                "Urza, Planeswalker",
            ]
        },
        manual_overrides={},
    )


# ---------------------------------------------------------------------------
# Assembly-level helpers
# ---------------------------------------------------------------------------

def _make_sku_ids(
    card_uuid: str,
    finishes: list[str],
    language: str = "English",
) -> dict[str, str | None]:
    """Compute skuIds struct matching pipeline output."""
    import uuid as _uuid

    all_finishes = ["nonfoil", "foil", "etched", "signed", "other"]
    return {
        f: str(_uuid.uuid5(_uuid.NAMESPACE_DNS, f"{card_uuid}_{f}_{language}")) if f in finishes else None
        for f in all_finishes
    }


_IDENTIFIERS_EMPTY: dict[str, Any] = {
    "abuId": None,
    "cardKingdomEtchedId": None,
    "cardKingdomFoilId": None,
    "cardKingdomId": None,
    "cardsphereFoilId": None,
    "cardsphereId": None,
    "cardtraderId": None,
    "csiId": None,
    "deckboxId": None,
    "mcmId": None,
    "mcmMetaId": None,
    "miniaturemarketId": None,
    "mtgArenaId": None,
    "mtgjsonFoilVersionId": None,
    "mtgjsonNonFoilVersionId": None,
    "mtgjsonV4Id": None,
    "mtgoFoilId": None,
    "mtgoId": None,
    "multiverseId": None,
    "mvpId": None,
    "scgId": None,
    "scryfallCardBackId": None,
    "scryfallId": None,
    "scryfallIllustrationId": None,
    "scryfallOracleId": None,
    "tcgplayerAlternativeFoilProductId": None,
    "tcgplayerEtchedProductId": None,
    "tcgplayerProductId": None,
    "tntId": None,
}

_LEGALITIES_EMPTY: dict[str, Any] = {
    "alchemy": None,
    "brawl": None,
    "commander": None,
    "duel": None,
    "explorer": None,
    "future": None,
    "gladiator": None,
    "historic": None,
    "historicbrawl": None,
    "legacy": None,
    "modern": None,
    "oathbreaker": None,
    "oldschool": None,
    "pauper": None,
    "paupercommander": None,
    "penny": None,
    "pioneer": None,
    "predh": None,
    "premodern": None,
    "standard": None,
    "standardbrawl": None,
    "timeless": None,
    "vintage": None,
}

_PURCHASE_URLS_EMPTY: dict[str, Any] = {
    "cardKingdom": None,
    "cardKingdomEtched": None,
    "cardKingdomFoil": None,
    "cardmarket": None,
    "tcgplayer": None,
    "tcgplayerAlternativeFoil": None,
    "tcgplayerEtched": None,
}


def make_assembled_card(**overrides: Any) -> dict[str, Any]:
    """Build a dict matching CardSet.polars_schema() for assembly-level tests.

    All fields are present with sensible defaults. Override only what matters.
    """
    card_uuid = overrides.get("uuid", "test-uuid-001")
    card_finishes = overrides.get("finishes", ["nonfoil"])
    defaults: dict[str, Any] = {
        # -- scalars --
        "artist": "Test Artist",
        "asciiName": None,
        "borderColor": "black",
        "convertedManaCost": 2.0,
        "defense": None,
        "duelDeck": None,
        "edhrecRank": None,
        "edhrecSaltiness": None,
        "faceConvertedManaCost": None,
        "faceFlavorName": None,
        "faceManaValue": None,
        "faceName": None,
        "facePrintedName": None,
        "flavorName": None,
        "flavorText": None,
        "frameVersion": "2015",
        "hand": None,
        "hasAlternativeDeckLimit": None,
        "hasContentWarning": None,
        "isAlternative": None,
        "isFullArt": None,
        "isFunny": None,
        "isGameChanger": None,
        "isOnlineOnly": None,
        "isOversized": None,
        "isPromo": None,
        "isRebalanced": None,
        "isReprint": None,
        "isReserved": None,
        "isStorySpotlight": None,
        "isTextless": None,
        "isTimeshifted": None,
        "language": "English",
        "layout": "normal",
        "life": None,
        "loyalty": None,
        "manaCost": "{1}{R}",
        "manaValue": 2.0,
        "name": "Test Card",
        "number": "1",
        "originalReleaseDate": None,
        "originalText": None,
        "originalType": None,
        "power": None,
        "printedName": None,
        "printedText": None,
        "printedType": None,
        "rarity": "common",
        "securityStamp": None,
        "setCode": "TST",
        "side": None,
        "signature": None,
        "text": "Test card text.",
        "toughness": None,
        "type": "Instant",
        "uuid": card_uuid,
        "watermark": None,
        # -- lists --
        "artistIds": None,
        "attractionLights": None,
        "availability": ["paper"],
        "boosterTypes": None,
        "cardParts": None,
        "colorIdentity": ["R"],
        "colorIndicator": None,
        "colors": ["R"],
        "finishes": card_finishes,
        "frameEffects": None,
        "keywords": None,
        "originalPrintings": None,
        "otherFaceIds": None,
        "printings": None,
        "producedMana": None,
        "promoTypes": None,
        "rebalancedPrintings": None,
        "subsets": None,
        "subtypes": [],
        "supertypes": [],
        "types": ["Instant"],
        "variations": None,
        # -- structs --
        "skuIds": _make_sku_ids(card_uuid, card_finishes),
        "identifiers": {**_IDENTIFIERS_EMPTY, "scryfallId": "sf-001", "scryfallOracleId": "oracle-001"},
        "legalities": {**_LEGALITIES_EMPTY, "vintage": "Legal", "legacy": "Legal", "commander": "Legal"},
        "leadershipSkills": None,
        "purchaseUrls": _PURCHASE_URLS_EMPTY.copy(),
        "relatedCards": None,
        "sourceProducts": None,
        # -- complex lists --
        "foreignData": None,
        "rulings": None,
    }
    defaults.update(overrides)
    return defaults


def make_assembled_token(**overrides: Any) -> dict[str, Any]:
    """Build a dict matching CardToken.polars_schema() for assembly-level tests."""
    token_uuid = overrides.get("uuid", "uuid-zombie-001")
    token_finishes = overrides.get("finishes", ["nonfoil"])
    defaults: dict[str, Any] = {
        "artist": "Token Artist",
        "artistIds": None,
        "asciiName": None,
        "attractionLights": None,
        "availability": ["paper"],
        "boosterTypes": None,
        "borderColor": "black",
        "cardParts": None,
        "colorIdentity": [],
        "colorIndicator": None,
        "colors": ["B"],
        "edhrecSaltiness": None,
        "faceFlavorName": None,
        "faceName": None,
        "facePrintedName": None,
        "finishes": token_finishes,
        "skuIds": _make_sku_ids(token_uuid, token_finishes),
        "flavorName": None,
        "flavorText": None,
        "frameEffects": None,
        "frameVersion": "2015",
        "identifiers": {**_IDENTIFIERS_EMPTY, "scryfallId": "sf-token-001"},
        "isFullArt": None,
        "isFunny": None,
        "isOnlineOnly": None,
        "isOversized": None,
        "isPromo": None,
        "isReprint": None,
        "isTextless": None,
        "keywords": None,
        "language": "English",
        "layout": "token",
        "loyalty": None,
        "manaCost": None,
        "name": "Zombie",
        "number": "T1",
        "orientation": None,
        "originalText": None,
        "originalType": None,
        "otherFaceIds": None,
        "power": "2",
        "printedName": None,
        "printedText": None,
        "printedType": None,
        "producedMana": None,
        "promoTypes": None,
        "relatedCards": None,
        "securityStamp": None,
        "setCode": "TTST",
        "side": None,
        "signature": None,
        "sourceProducts": None,
        "subsets": None,
        "subtypes": ["Zombie"],
        "supertypes": [],
        "text": None,
        "tokenProducts": None,
        "toughness": "2",
        "type": "Token Creature — Zombie",
        "types": ["Token", "Creature"],
        "uuid": token_uuid,
        "watermark": None,
    }
    defaults.update(overrides)
    return defaults


@pytest.fixture(scope="session")
def assembly_ctx(tmp_path_factory: pytest.TempPathFactory) -> AssemblyContext:
    """Build a tiny AssemblyContext from synthetic parquet fixtures."""
    from mtgjson5.build.context import AssemblyContext
    from mtgjson5.models.cards import CardSet, CardToken

    base = tmp_path_factory.mktemp("assembly")
    parquet_dir = base / "_parquet"
    tokens_dir = base / "_parquet_tokens"
    output_dir = base / "output"
    output_dir.mkdir()

    card_schema = CardSet.polars_schema()
    token_schema = CardToken.polars_schema()

    # --- TST set: 4 cards ---
    tst_cards = [
        make_assembled_card(
            name="Lightning Bolt",
            number="1",
            uuid="uuid-bolt-tst",
            type="Instant",
            types=["Instant"],
            text="Lightning Bolt deals 3 damage to any target.",
            manaCost="{R}",
            manaValue=1.0,
            convertedManaCost=1.0,
            colors=["R"],
            colorIdentity=["R"],
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-bolt-tst", "scryfallOracleId": "oracle-bolt"},
            legalities={
                **_LEGALITIES_EMPTY,
                "vintage": "Legal",
                "legacy": "Legal",
                "modern": "Legal",
                "commander": "Legal",
            },
        ),
        make_assembled_card(
            name="Wear // Tear",
            number="2a",
            uuid="uuid-wear",
            layout="split",
            side="a",
            faceName="Wear",
            type="Instant",
            types=["Instant"],
            text="Destroy target artifact.",
            manaCost="{R}",
            manaValue=3.0,
            convertedManaCost=3.0,
            colors=["R"],
            colorIdentity=["R", "W"],
            otherFaceIds=["uuid-tear"],
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-wt", "scryfallOracleId": "oracle-wt"},
            legalities={**_LEGALITIES_EMPTY, "vintage": "Legal", "legacy": "Legal", "commander": "Legal"},
        ),
        make_assembled_card(
            name="Wear // Tear",
            number="2b",
            uuid="uuid-tear",
            layout="split",
            side="b",
            faceName="Tear",
            type="Instant",
            types=["Instant"],
            text="Destroy target enchantment.",
            manaCost="{W}",
            manaValue=3.0,
            convertedManaCost=3.0,
            colors=["W"],
            colorIdentity=["R", "W"],
            otherFaceIds=["uuid-wear"],
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-wt", "scryfallOracleId": "oracle-wt"},
            legalities={**_LEGALITIES_EMPTY, "vintage": "Legal", "legacy": "Legal", "commander": "Legal"},
        ),
        make_assembled_card(
            name="Funny Card",
            number="3",
            uuid="uuid-funny",
            type="Creature — Clown",
            types=["Creature"],
            subtypes=["Clown"],
            text="This card is funny.",
            isFunny=True,
            power="1",
            toughness="1",
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-funny", "scryfallOracleId": "oracle-funny"},
            legalities=_LEGALITIES_EMPTY.copy(),
        ),
    ]

    # --- TS2 set: 2 cards (Lightning Bolt reprint + Prodigal Sorcerer) ---
    ts2_cards = [
        make_assembled_card(
            name="Lightning Bolt",
            number="1",
            uuid="uuid-bolt-ts2",
            setCode="TS2",
            type="Instant",
            types=["Instant"],
            text="Lightning Bolt deals 3 damage to any target.",
            manaCost="{R}",
            manaValue=1.0,
            convertedManaCost=1.0,
            colors=["R"],
            colorIdentity=["R"],
            isReprint=True,
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-bolt-ts2", "scryfallOracleId": "oracle-bolt"},
            legalities={
                **_LEGALITIES_EMPTY,
                "vintage": "Legal",
                "legacy": "Legal",
                "modern": "Legal",
                "commander": "Legal",
                "pauper": "Legal",
            },
        ),
        make_assembled_card(
            name="Prodigal Sorcerer",
            number="2",
            uuid="uuid-sorcerer",
            setCode="TS2",
            type="Creature — Human Wizard",
            types=["Creature"],
            subtypes=["Human", "Wizard"],
            text="{T}: Prodigal Sorcerer deals 1 damage to any target.",
            manaCost="{2}{U}",
            manaValue=3.0,
            convertedManaCost=3.0,
            colors=["U"],
            colorIdentity=["U"],
            power="1",
            toughness="1",
            identifiers={**_IDENTIFIERS_EMPTY, "scryfallId": "sf-sorcerer", "scryfallOracleId": "oracle-sorcerer"},
            legalities={**_LEGALITIES_EMPTY, "vintage": "Legal", "legacy": "Legal", "commander": "Legal"},
        ),
    ]

    # --- Token ---
    token = make_assembled_token()

    # Write parquet partitions
    for code, cards in [("TST", tst_cards), ("TS2", ts2_cards)]:
        path = parquet_dir / f"setCode={code}"
        path.mkdir(parents=True)
        pl.DataFrame(cards, schema=card_schema).write_parquet(path / "0.parquet")

    t_path = tokens_dir / "setCode=TTST"
    t_path.mkdir(parents=True)
    pl.DataFrame([token], schema=token_schema).write_parquet(t_path / "0.parquet")

    set_meta = {
        "TST": {
            "code": "TST",
            "name": "Test Set",
            "type": "expansion",
            "releaseDate": "2025-01-01",
            "keyruneCode": "TST",
            "isFoilOnly": False,
            "isOnlineOnly": False,
            "tokenSetCode": "TTST",
            "translations": {},
        },
        "TS2": {
            "code": "TS2",
            "name": "Test Set Two",
            "type": "expansion",
            "releaseDate": "2025-06-01",
            "keyruneCode": "TS2",
            "isFoilOnly": False,
            "isOnlineOnly": False,
            "tokenSetCode": None,
            "translations": {},
        },
    }

    return AssemblyContext(
        parquet_dir=parquet_dir,
        tokens_dir=tokens_dir,
        set_meta=set_meta,
        meta={"date": "2025-01-01", "version": "5.3.0+test"},
        output_path=output_dir,
    )
