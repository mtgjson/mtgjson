"""Data assembly utilities for output generation."""

from __future__ import annotations

import pathlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import polars as pl

from mtgjson5.mtgjson_models.cards import CardAtomic, CardDeck, CardSet, CardToken
from mtgjson5.mtgjson_models.sets import SealedProduct


if TYPE_CHECKING:
    from .context import AssemblyContext


class Assembler:
    """Load data from parquet cache."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def load_set_cards(self, code: str) -> pl.DataFrame:
        """Load cards for a specific set."""
        path = self.ctx.parquet_dir / f"setCode={code}"
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path / "*.parquet")

    def load_set_tokens(self, code: str) -> pl.DataFrame:
        """Load tokens for a specific set.

        Checks multiple possible token directories in order:
        1. tokenSetCode from set metadata (if specified)
        2. T{code} (standard token set prefix)
        3. {code} itself (for sets like WC00 where tokens share the set code)
        """
        meta = self.ctx.set_meta.get(code, {})

        # Build list of candidate token codes to check
        candidates = []
        if meta.get("tokenSetCode"):
            candidates.append(meta["tokenSetCode"])
        candidates.append(f"T{code}")
        candidates.append(code)

        # Try each candidate in order
        for token_code in candidates:
            path = self.ctx.tokens_dir / f"setCode={token_code}"
            if path.exists():
                return pl.read_parquet(path / "*.parquet")

        return pl.DataFrame()

    def load_all_cards(self) -> pl.LazyFrame:
        """Load all cards as LazyFrame."""
        return pl.scan_parquet(self.ctx.parquet_dir / "**/*.parquet")

    def load_all_tokens(self) -> pl.LazyFrame:
        """Load all tokens as LazyFrame."""
        return pl.scan_parquet(self.ctx.tokens_dir / "**/*.parquet")

    def get_set_metadata(self, code: str) -> dict[str, Any]:
        """Get metadata for a specific set."""
        return self.ctx.set_meta.get(code, {})

    def iter_set_codes(self) -> list[str]:
        """Get list of all set codes to include in output.

        Includes:
        - Sets with card parquet data
        - Sets with token parquet data (e.g., MSTX, L14, PR2, F18)
        - Sets with only metadata (e.g., Q01, DD3, FWB, MB1 from additional_sets.json)

        Excludes traditional token sets (type='token' AND code starts with 'T')
        like TONE, TC15, T10E. These are product token inserts.

        Keeps special token sets like L14 (League Tokens), SBRO (Substitute Cards),
        WMOM (Japanese Promo Tokens), etc. that don't start with 'T'.
        """
        # Sets with card parquet data
        card_sets = {
            p.name.replace("setCode=", "")
            for p in self.ctx.parquet_dir.iterdir()
            if p.is_dir() and p.name.startswith("setCode=")
        }

        # Sets with token parquet data
        token_sets: set[str] = set()
        if self.ctx.tokens_dir.exists():
            token_sets = {
                p.name.replace("setCode=", "")
                for p in self.ctx.tokens_dir.iterdir()
                if p.is_dir() and p.name.startswith("setCode=")
            }

        # Sets with only metadata (from additional_sets.json or sealed products)
        metadata_only_sets = set(self.ctx.set_meta.keys())

        # Combine all sources
        all_sets = card_sets | token_sets | metadata_only_sets

        # Filter out traditional token sets (type='token' AND code starts with 'T')
        # Keep special token sets like L14, SBRO, WMOM that don't start with 'T'
        return sorted(
            code for code in all_sets
            if not (
                self.ctx.set_meta.get(code, {}).get("type") == "token"
                and code.startswith("T")
            )
        )


class AtomicCardsAssembler(Assembler):
    """Assembles AtomicCards grouped by name."""

    @staticmethod
    def _strip_none_recursive(obj: Any) -> Any:
        """Recursively strip None values from dicts and lists.

        Polars struct columns contain None for missing values, but Pydantic
        TypedDicts don't accept None where a string is expected.
        """
        if isinstance(obj, dict):
            return {k: AtomicCardsAssembler._strip_none_recursive(v)
                    for k, v in obj.items() if v is not None}
        if isinstance(obj, list):
            return [AtomicCardsAssembler._strip_none_recursive(v)
                    for v in obj if v is not None]
        return obj

    def iter_atomic(self) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        """Iterate over atomic cards grouped by name."""
        atomic_schema = CardAtomic.polars_schema()
        atomic_fields = set(atomic_schema.keys())

        lf = self.load_all_cards()
        available = set(lf.collect_schema().names())
        select_cols = list(atomic_fields & available)

        if "name" not in select_cols:
            select_cols.append("name")

        df = (
            lf.select(select_cols)
            .unique(subset=["name", "colorIdentity", "manaCost", "type"])
            .sort("name")
            .collect()
        )

        current_name: str | None = None
        current_cards: list[dict[str, Any]] = []

        for row in df.to_dicts():
            name = row.get("name", "")

            if name != current_name:
                if current_name is not None and current_cards:
                    yield current_name, current_cards
                current_name = name
                current_cards = []

            # Strip None values from nested structs before validation
            cleaned = self._strip_none_recursive(row)
            try:
                model = CardAtomic.from_polars_row(cleaned)
                current_cards.append(model.to_polars_dict(exclude_none=True))
            except Exception:
                pass

        if current_name is not None and current_cards:
            yield current_name, current_cards

    def build(self) -> dict[str, list[dict[str, Any]]]:
        """Build complete AtomicCards data dict."""
        return dict(self.iter_atomic())


class DeckAssembler:
    """Assembles Deck objects with expanded card data."""

    def __init__(self, cards_df: pl.DataFrame):
        self.cards_df = cards_df
        self._uuid_index: dict[str, dict[str, Any]] | None = None

    @property
    def uuid_index(self) -> dict[str, dict[str, Any]]:
        """Lazy-build UUID -> card dict index.

        Uses CardSet (not CardDeck) since we're loading base card data.
        Deck-specific fields (count, isFoil, isEtched) are added in expand_card_list().
        """
        if self._uuid_index is None:
            models = CardSet.from_dataframe(self.cards_df)
            self._uuid_index = {
                m.uuid: m.to_polars_dict(exclude_none=True) for m in models
            }
        return self._uuid_index

    def expand_card_list(self, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Expand card references to full card objects.

        Uses CardDeck model to serialize, which excludes deck-inappropriate fields
        via exclude=True on field definitions (boosterTypes, keywords, rulings, etc).
        """
        result = []
        for ref in refs:
            uuid = ref.get("uuid")
            if not uuid:
                continue

            card = self.uuid_index.get(uuid)
            if card is None:
                continue

            # Add deck-specific fields to the card dict
            expanded = dict(card)
            expanded["count"] = ref.get("count", 1)
            expanded["isFoil"] = ref.get("isFoil", False)
            expanded["isEtched"] = ref.get("isEtched", False)

            # Use from_polars_row to convert aliased dict to CardDeck instance,
            # then to_polars_dict which respects exclude=True on fields
            deck_card = CardDeck.from_polars_row(expanded)
            result.append(deck_card.to_polars_dict(exclude_none=True))

        return result

    def build(self, deck_data: dict[str, Any]) -> dict[str, Any]:
        """Build a complete Deck with expanded cards."""
        result = {
            "code": deck_data.get("code", deck_data.get("setCode", "")),
            "name": deck_data.get("name", ""),
            "type": deck_data.get("type", ""),
            "releaseDate": deck_data.get("releaseDate"),
        }

        if deck_data.get("sealedProductUuids"):
            result["sealedProductUuids"] = deck_data["sealedProductUuids"]

        for board in ["mainBoard", "sideBoard", "commander", "tokens"]:
            refs = deck_data.get(board, [])
            if refs:
                result[board] = self.expand_card_list(refs)
            elif board in ["mainBoard", "sideBoard"]:
                result[board] = []

        return result

    def to_dataframe(self, deck_data: dict[str, Any]) -> pl.DataFrame:
        """Build a deck and return as a flattened DataFrame for parquet/csv export.

        Returns a DataFrame with one row per card in the deck, with columns:
        - deck_code, deck_name, deck_type, release_date
        - board (mainBoard, sideBoard, commander, tokens)
        - count, uuid, and all card fields
        """
        deck = self.build(deck_data)
        rows = []

        base = {
            "deck_code": deck.get("code", ""),
            "deck_name": deck.get("name", ""),
            "deck_type": deck.get("type", ""),
            "release_date": deck.get("releaseDate"),
        }

        for board in ["mainBoard", "sideBoard", "commander", "tokens"]:
            cards = deck.get(board, [])
            for card in cards:
                row = {**base, "board": board, **card}
                rows.append(row)

        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def build_all_dataframe(self, decks_df: pl.DataFrame) -> pl.DataFrame:
        """Build all decks as a single DataFrame."""
        dfs = []
        for deck_raw in decks_df.to_dicts():
            df = self.to_dataframe(deck_raw)
            if not df.is_empty():
                dfs.append(df)
        return pl.concat(dfs) if dfs else pl.DataFrame()

    @classmethod
    def from_parquet(
        cls,
        parquet_dir: pathlib.Path,
        set_codes: list[str] | None = None,
    ) -> DeckAssembler:
        """Create assembler with cards loaded from parquet."""
        if set_codes:
            dfs = []
            for code in set_codes:
                path = parquet_dir / f"setCode={code}"
                if path.exists():
                    dfs.append(pl.read_parquet(path / "*.parquet"))
            cards_df = pl.concat(dfs) if dfs else pl.DataFrame()
        else:
            cards_df = pl.read_parquet(parquet_dir / "**/*.parquet")

        return cls(cards_df)


class DeckListAssembler(Assembler):
    """Assembles DeckList.json (deck summaries without cards)."""

    def build(self) -> list[dict[str, Any]]:
        """Build DeckList entries."""
        if self.ctx.decks_df is None:
            return []

        deck_list = []
        for deck in self.ctx.decks_df.to_dicts():
            set_code = deck.get("setCode", "")
            name = deck.get("name", "")
            safe_name = "".join(c for c in name if c.isalnum())

            entry = {
                "code": set_code,
                "fileName": f"{safe_name}_{set_code}",
                "name": name,
                "type": deck.get("type", ""),
            }
            if deck.get("releaseDate"):
                entry["releaseDate"] = deck["releaseDate"]

            deck_list.append(entry)

        return sorted(deck_list, key=lambda d: (d["code"], d["name"]))


class SetAssembler(Assembler):
    """Assembles complete Set objects from parquet data."""

    def get_cards(self, set_code: str) -> list[dict[str, Any]]:
        """Load and serialize cards for a set, sorted by collector number."""
        df = self.load_set_cards(set_code)
        if df.is_empty():
            return []
        models = CardSet.from_dataframe(df)
        models.sort()
        return [m.to_polars_dict(exclude_none=True) for m in models]

    def get_tokens(self, set_code: str) -> list[dict[str, Any]]:
        """Load and serialize tokens for a set, sorted by collector number."""
        df = self.load_set_tokens(set_code)
        if df.is_empty():
            return []
        models = CardToken.from_dataframe(df)
        models.sort()
        return [m.to_polars_dict(exclude_none=True) for m in models]

    def build(
        self,
        set_code: str,
        include_decks: bool = True,
        include_sealed: bool = True,
        include_booster: bool = True,
    ) -> dict[str, Any]:
        """Build a complete Set dict."""
        meta = self.get_set_metadata(set_code)
        cards = self.get_cards(set_code)
        tokens = self.get_tokens(set_code)

        # Clean translations (remove None values)
        translations_raw = meta.get("translations", {})
        translations = (
            {k: v for k, v in translations_raw.items() if v is not None}
            if translations_raw
            else {}
        )

        # Base set structure
        set_data: dict[str, Any] = {
            "baseSetSize": meta.get("baseSetSize")
            or len([c for c in cards if not c.get("isReprint")])
            or len(cards),
            "cards": cards,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "tokens": tokens,
            "totalSetSize": meta.get("totalSetSize") or len(cards),
            "translations": translations,
            "type": meta.get("type", ""),
        }

        # Optional fields - only include if present and truthy (booleans only when True)
        for fld in [
            "block",
            "parentCode",
            "mtgoCode",
            "tokenSetCode",
            "tcgplayerGroupId",
            "cardsphereSetId",
            "mcmId",
            "mcmIdExtras",
            "mcmName",
            "isPaperOnly",
            "isForeignOnly",
            "isNonFoilOnly",
            "isPartialPreview",
        ]:
            val = meta.get(fld)
            # Exclude None and False (MTGJSON convention: only include True booleans)
            if val is not None and val is not False:
                set_data[fld] = val

        # Languages from foreign data
        languages = {"English"}
        for card in cards:
            for fd in card.get("foreignData", []):
                if fd.get("language"):
                    languages.add(fd["language"])
        set_data["languages"] = sorted(languages)

        # Booster config
        if include_booster and set_code in self.ctx.booster_configs:
            set_data["booster"] = self.ctx.booster_configs[set_code]

        # Decks (minimal format)
        if include_decks and self.ctx.decks_df is not None:
            set_decks = self.ctx.decks_df.filter(pl.col("setCode") == set_code)
            if len(set_decks) > 0:
                minimal_decks = []
                for deck in set_decks.to_dicts():
                    minimal_deck = {
                        "code": deck.get("code", set_code),
                        "name": deck.get("name", ""),
                        "type": deck.get("type", ""),
                    }
                    if deck.get("releaseDate"):
                        minimal_deck["releaseDate"] = deck["releaseDate"]
                    if deck.get("sealedProductUuids"):
                        minimal_deck["sealedProductUuids"] = deck["sealedProductUuids"]
                    for board in ["mainBoard", "sideBoard", "commander"]:
                        cards_list = deck.get(board)
                        if cards_list:
                            minimal_deck[board] = [
                                {
                                    k: v
                                    for k, v in c.items()
                                    if k in ("count", "uuid", "isFoil")
                                    and v not in (None, False)
                                }
                                for c in cards_list
                                if isinstance(c, dict)
                            ]
                        else:
                            minimal_deck[board] = []
                    minimal_decks.append(minimal_deck)
                set_data["decks"] = minimal_decks

        # Sealed products
        if include_sealed and self.ctx.sealed_df is not None:
            set_sealed = self.ctx.sealed_df.filter(pl.col("setCode") == set_code)
            if len(set_sealed) > 0:
                models = SealedProduct.from_dataframe(set_sealed.drop("setCode"))
                set_data["sealedProduct"] = [
                    m.to_polars_dict(exclude_none=True) for m in models
                ]

        return set_data

    def iter_sets(
        self,
        set_codes: list[str] | None = None,
        **kwargs: Any,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Iterate over built sets."""
        available = set(self.iter_set_codes())
        codes = set_codes or sorted(available)
        for code in codes:
            if code in available:
                yield code, self.build(code, **kwargs)


class SetListAssembler(Assembler):
    """Assembles SetList (set summaries without cards)."""

    def build_one(self, set_code: str) -> dict[str, Any]:
        """Build a single SetList entry."""
        meta = self.get_set_metadata(set_code)
        df = self.load_set_cards(set_code)
        card_count = len(df) if not df.is_empty() else 0

        # Clean translations (remove None values)
        translations_raw = meta.get("translations", {})
        translations = (
            {k: v for k, v in translations_raw.items() if v is not None}
            if translations_raw
            else {}
        )

        return {
            "baseSetSize": meta.get("baseSetSize") or card_count,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "totalSetSize": meta.get("totalSetSize") or card_count,
            "translations": translations,
            "type": meta.get("type", ""),
            **{
                k: v
                for k in [
                    "block",
                    "parentCode",
                    "mtgoCode",
                    "tokenSetCode",
                    "tcgplayerGroupId",
                    "cardsphereSetId",
                    "mcmId",
                    "mcmIdExtras",
                    "mcmName",
                    "isPaperOnly",
                    "isForeignOnly",
                    "isNonFoilOnly",
                    "isPartialPreview",
                ]
                # Exclude None and False (MTGJSON convention: only include True booleans)
                if (v := meta.get(k)) is not None and v is not False
            },
        }

    def build(self) -> list[dict[str, Any]]:
        """Build all SetList entries."""
        return [self.build_one(code) for code in self.iter_set_codes()]


class TableAssembler:
    """Build normalized relational tables from card data."""

    @staticmethod
    def build_all(
        cards_df: pl.DataFrame,
        tokens_df: pl.DataFrame | None = None,
        sets_df: pl.DataFrame | None = None,
    ) -> dict[str, pl.DataFrame]:
        """
        Build all normalized tables for SQL/CSV export.

        Returns:
            Dict mapping table name to DataFrame
        """
        tables: dict[str, pl.DataFrame] = {}
        schema = cards_df.schema

        # Find ALL nested columns (List or Struct types)
        nested = {
            c for c in cards_df.columns if isinstance(schema[c], pl.List | pl.Struct)
        }
        scalar_cols = [
            c for c in cards_df.columns if c not in nested and not c.startswith("_")
        ]

        # cards - scalar columns
        tables["cards"] = cards_df.select(scalar_cols)

        # cardIdentifiers - unnest struct
        if "identifiers" in schema and isinstance(schema["identifiers"], pl.Struct):
            tables["cardIdentifiers"] = (
                cards_df.select("uuid", "identifiers")
                .filter(pl.col("identifiers").is_not_null())
                .unnest("identifiers")
            )

        # cardLegalities - unnest struct
        if "legalities" in schema and isinstance(schema["legalities"], pl.Struct):
            tables["cardLegalities"] = (
                cards_df.select("uuid", "legalities")
                .filter(pl.col("legalities").is_not_null())
                .unnest("legalities")
            )

        # cardForeignData - explode list of structs
        if "foreignData" in schema and isinstance(schema["foreignData"], pl.List):
            tables["cardForeignData"] = (
                cards_df.select("uuid", "foreignData")
                .filter(pl.col("foreignData").list.len() > 0)
                .explode("foreignData")
                .unnest("foreignData")
            )

        # cardRulings - explode list of structs
        if "rulings" in schema and isinstance(schema["rulings"], pl.List):
            tables["cardRulings"] = (
                cards_df.select("uuid", "rulings")
                .filter(pl.col("rulings").list.len() > 0)
                .explode("rulings")
                .unnest("rulings")
            )

        # cardPurchaseUrls - unnest struct
        if "purchaseUrls" in schema and isinstance(schema["purchaseUrls"], pl.Struct):
            tables["cardPurchaseUrls"] = (
                cards_df.select("uuid", "purchaseUrls")
                .filter(pl.col("purchaseUrls").is_not_null())
                .unnest("purchaseUrls")
            )

        # tokens
        if tokens_df is not None and len(tokens_df) > 0:
            token_schema = tokens_df.schema
            token_scalar = [
                c for c in tokens_df.columns
                if not isinstance(token_schema.get(c), pl.List | pl.Struct)
                and not c.startswith("_")
            ]
            tables["tokens"] = tokens_df.select(token_scalar)

            if "identifiers" in token_schema and isinstance(
                token_schema["identifiers"], pl.Struct
            ):
                tables["tokenIdentifiers"] = (
                    tokens_df.select("uuid", "identifiers")
                    .filter(pl.col("identifiers").is_not_null())
                    .unnest("identifiers")
                )

        # sets
        if sets_df is not None and len(sets_df) > 0:
            sets_schema = sets_df.schema
            sets_nested = {
                c for c in sets_df.columns
                if isinstance(sets_schema[c], pl.List | pl.Struct)
            }
            sets_scalar = [
                c for c in sets_df.columns
                if c not in sets_nested and not c.startswith("_")
            ]
            tables["sets"] = sets_df.select(sets_scalar)

            # setTranslations
            if "translations" in sets_schema and isinstance(
                sets_schema["translations"], pl.Struct
            ):
                trans_df = (
                    sets_df.select("code", "translations")
                    .filter(pl.col("translations").is_not_null())
                    .unnest("translations")
                )
                if len(trans_df) > 0:
                    tables["setTranslations"] = trans_df

        return tables

    @staticmethod
    def build_boosters(booster_configs: dict[str, dict[str, Any]]) -> dict[str, pl.DataFrame]:
        """
        Build booster configuration tables.

        Returns:
            Dict with tables: setBoosterSheets, setBoosterSheetCards,
            setBoosterContents, setBoosterContentWeights
        """
        sheets_records: list[dict] = []
        sheet_cards_records: list[dict] = []
        contents_records: list[dict] = []
        weights_records: list[dict] = []

        for set_code, config in booster_configs.items():
            if not isinstance(config, dict):
                continue

            for booster_name, booster_data in config.items():
                if not isinstance(booster_data, dict):
                    continue

                # Parse sheets
                sheets = booster_data.get("sheets", {})
                for sheet_name, sheet_data in sheets.items():
                    if not isinstance(sheet_data, dict):
                        continue

                    sheets_records.append({
                        "setCode": set_code,
                        "boosterName": booster_name,
                        "sheetName": sheet_name,
                        "sheetIsFoil": sheet_data.get("foil", False),
                        "sheetHasBalanceColors": sheet_data.get("balanceColors", False),
                        "sheetTotalWeight": sheet_data.get("totalWeight", 0),
                    })

                    # Parse cards in sheet
                    cards = sheet_data.get("cards", {})
                    for card_uuid, weight in cards.items():
                        sheet_cards_records.append({
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "sheetName": sheet_name,
                            "cardUuid": card_uuid,
                            "cardWeight": weight,
                        })

                # Parse boosters (contents and weights)
                boosters = booster_data.get("boosters", [])
                for idx, booster_variant in enumerate(boosters):
                    if not isinstance(booster_variant, dict):
                        continue

                    booster_weight = booster_variant.get("weight", 1)
                    weights_records.append({
                        "setCode": set_code,
                        "boosterName": booster_name,
                        "boosterIndex": idx,
                        "boosterWeight": booster_weight,
                    })

                    contents = booster_variant.get("contents", {})
                    for sheet_name, picks in contents.items():
                        contents_records.append({
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "boosterIndex": idx,
                            "sheetName": sheet_name,
                            "sheetPicks": picks,
                        })

        return {
            "setBoosterSheets": pl.DataFrame(sheets_records) if sheets_records else pl.DataFrame(),
            "setBoosterSheetCards": pl.DataFrame(sheet_cards_records) if sheet_cards_records else pl.DataFrame(),
            "setBoosterContents": pl.DataFrame(contents_records) if contents_records else pl.DataFrame(),
            "setBoosterContentWeights": pl.DataFrame(weights_records) if weights_records else pl.DataFrame(),
        }
