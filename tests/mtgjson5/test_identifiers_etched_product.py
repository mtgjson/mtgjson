"""An etched product id stays on the cards sold etched."""

import polars as pl
import pytest

from mtgjson5.pipeline.stages.identifiers import add_identifiers_struct

_FACE = pl.Struct({"oracle_id": pl.String, "illustration_id": pl.String})

# Every column add_identifiers_struct reads, empty but for the ones set per row
_EMPTY: dict[str, pl.DataType | type[pl.DataType]] = {
    "_face_data": _FACE,
    "oracleId": pl.String,
    "illustrationId": pl.String,
    "cardBackId": pl.String,
    "mcmId": pl.String,
    "mcmMetaId": pl.String,
    "arenaId": pl.Int64,
    "mtgoId": pl.Int64,
    "mtgoFoilId": pl.Int64,
    "multiverseIds": pl.List(pl.Int64),
    "faceId": pl.Int64,
    "tcgplayerAlternativeFoilProductId": pl.String,
    "cardKingdomId": pl.String,
    "cardKingdomFoilId": pl.String,
    "cardKingdomEtchedId": pl.String,
    "cardsphereId": pl.String,
    "cardsphereAlternativeFoilId": pl.String,
    "cardsphereEtchedId": pl.String,
    "cardsphereFoilId": pl.String,
    "deckboxId": pl.String,
}


# Scryfall's own values for each card
@pytest.mark.parametrize(
    ("card", "scryfall_id", "finishes", "tcgplayer_id", "etched_id", "expected"),
    [
        ("SLD #159", "27197660-8489-419b-9ad6-29a8713e4673", ["nonfoil", "foil"], 251772, 251773, None),
        ("SLD #159★", "5d58cb4d-2091-40c8-b97c-09bf9c022a8b", ["etched"], None, 251773, "251773"),
        ("STA #10", "cc9ece2f-7eda-4fc5-a562-3e16e71560e9", ["nonfoil", "foil", "etched"], 233369, 233370, "233370"),
        ("PFDN #1", "120e2b4b-afc7-4bf0-a09f-568e08f6bd8f", ["foil"], 594545, 594545, None),
        ("40K #173", "1956cebe-bb71-45cc-b4ff-72ee3bb95c81", ["foil"], 285791, 286677, None),
    ],
)
def test_etched_product_id_needs_an_etched_finish(card, scryfall_id, finishes, tcgplayer_id, etched_id, expected):
    frame = pl.DataFrame(
        {
            "scryfallId": [scryfall_id],
            "finishes": [finishes],
            "tcgplayerId": [tcgplayer_id],
            "tcgplayerEtchedId": [etched_id],
            **{name: pl.Series(name, [None], dtype=dtype) for name, dtype in _EMPTY.items()},
        },
        schema_overrides={"tcgplayerId": pl.Int64, "tcgplayerEtchedId": pl.Int64},
    )
    identifiers = add_identifiers_struct(frame.lazy()).collect()["identifiers"][0]
    assert identifiers["tcgplayerEtchedProductId"] == expected, card
    assert identifiers["tcgplayerProductId"] == (None if tcgplayer_id is None else str(tcgplayer_id)), card
