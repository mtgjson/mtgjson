"""Data assembly utilities for output generation."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import polars as pl

from mtgjson5.v2.consts.fields import (
    ATOMIC_FOREIGN_STRIP,
    ATOMIC_IDENTIFIERS,
    ORACLE_IDENTITY_COLS,
)
from mtgjson5.v2.models.cards import CardAtomic, CardDeck, CardSet, CardToken
from mtgjson5.v2.models.schemas import (
    CARDS_TABLE_EXCLUDE,
    SETS_TABLE_EXCLUDE,
    TOKENS_TABLE_EXCLUDE,
)
from mtgjson5.v2.models.sets import SealedProduct

if TYPE_CHECKING:
    from .context import AssemblyContext

EXTRA_SORT_COLS = ["isOversized", "isPromo", "isOnlineOnly"]


def compute_format_legal_sets(
    ctx: AssemblyContext,
    format_name: str,
) -> set[str]:
    """
    Compute which sets are legal for a format.

    A set is format-legal if:
    1. Its type is in SUPPORTED_SET_TYPES (expansion, core, etc.)
    2. ALL non-Alchemy cards have the format key in their legalities struct

    Note: This checks for key PRESENCE (is_not_null), not for "Legal"/"Restricted" values.
    MTGJSON omits "not_legal" entries, so key presence indicates the card is part
    of the format (Legal, Restricted, or Banned).
    """
    from mtgjson5.v2.consts import SUPPORTED_SET_TYPES

    # Filter by set type using metadata
    valid_type_sets = {code for code, meta in ctx.set_meta.items() if meta.get("type", "") in SUPPORTED_SET_TYPES}

    lf = pl.scan_parquet(ctx.parquet_dir / "**/*.parquet")

    set_legality = (
        lf.filter(~pl.col("name").str.starts_with("A-"))
        .with_columns(pl.col("legalities").struct.field(format_name).is_not_null().alias("_has_format_key"))
        .group_by("setCode")
        .agg(pl.col("_has_format_key").all().alias("_all_have_key"))
        .filter(pl.col("_all_have_key"))
        .select("setCode")
        .collect()
    )

    format_legal = set(set_legality["setCode"].to_list())
    return format_legal & valid_type_sets


class Assembler:
    """Load data from parquet cache."""

    def __init__(self, ctx: AssemblyContext):
        self.ctx = ctx

    def load_set_cards(self, code: str) -> pl.DataFrame:
        """Load cards for a specific set."""
        from mtgjson5.v2.utils import get_windows_safe_set_code

        safe_code = get_windows_safe_set_code(code)
        path = self.ctx.parquet_dir / f"setCode={safe_code}"
        if not path.exists():
            return pl.DataFrame()
        df = pl.read_parquet(path / "*.parquet")
        return df.unique(subset=["uuid"]) if "uuid" in df.columns else df

    def load_set_tokens(self, code: str) -> pl.DataFrame:
        """Load tokens for a specific set.

        Checks multiple possible token directories in order:
        1. tokenSetCode from set metadata (if specified)
        2. T{code} (standard token set prefix)
        3. {code} itself (for sets like WC00 where tokens share the set code)

        Remaps the token setCode to the parent set code so tokens
        reference an actual MTGJSON set (e.g. TZNR -> ZNR).
        """
        from mtgjson5.v2.utils import get_windows_safe_set_code

        meta = self.ctx.set_meta.get(code, {})

        # Build list of candidate token codes to check
        candidates = []
        if meta.get("tokenSetCode"):
            candidates.append(meta["tokenSetCode"])
        candidates.append(f"T{code}")
        candidates.append(code)

        # Try each candidate in order
        for token_code in candidates:
            safe_code = get_windows_safe_set_code(token_code)
            path = self.ctx.tokens_dir / f"setCode={safe_code}"
            if path.exists():
                df = pl.read_parquet(path / "*.parquet")
                # Remap token setCode to the parent set code
                if "setCode" in df.columns and token_code != code:
                    df = df.with_columns(pl.lit(code).alias("setCode"))
                return df

        return pl.DataFrame()

    def load_all_cards(self) -> pl.LazyFrame:
        """Load all cards as LazyFrame."""
        lf = pl.scan_parquet(
            self.ctx.parquet_dir / "**/*.parquet",
            cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        )
        return lf.unique(subset=["uuid"])

    def load_all_tokens(self) -> pl.LazyFrame:
        """Load all tokens as LazyFrame."""
        return pl.scan_parquet(self.ctx.tokens_dir / "**/*.parquet")

    def get_set_metadata(self, code: str) -> dict[str, Any]:
        """Get metadata for a specific set."""
        return self.ctx.set_meta.get(code, {})

    def build_languages(self, cards: list[dict[str, Any]]) -> list[str]:
        """Build sorted language list from card foreignData."""
        languages = {"English"}
        for card in cards:
            for fd in card.get("foreignData", []):
                if fd.get("language"):
                    languages.add(fd["language"])
        return sorted(languages)

    def build_minimal_decks(self, set_code: str) -> list[dict[str, Any]] | None:
        """Build minimal deck references for a set, or None if no decks."""
        if self.ctx.decks_df is None:
            return None
        set_decks = self.ctx.decks_df.filter(pl.col("setCode") == set_code)
        if len(set_decks) == 0:
            return None
        minimal_decks = []
        for deck in set_decks.to_dicts():
            minimal_deck: dict[str, Any] = {
                "code": deck.get("code", set_code),
                "name": deck.get("name", ""),
                "type": deck.get("type", ""),
            }
            if deck.get("releaseDate"):
                minimal_deck["releaseDate"] = deck["releaseDate"]
            sealed_uuids = deck.get("sealedProductUuids")
            minimal_deck["sealedProductUuids"] = sealed_uuids if sealed_uuids else None
            if deck.get("sourceSetCodes"):
                minimal_deck["sourceSetCodes"] = deck["sourceSetCodes"]
            for board in ["mainBoard", "sideBoard"]:
                cards_list = deck.get(board)
                if cards_list:
                    minimal_deck[board] = [
                        {
                            k: v
                            for k, v in c.items()
                            if k in ("count", "uuid", "isFoil", "isEtched") and v not in (None, False)
                        }
                        for c in cards_list
                        if isinstance(c, dict)
                    ]
                else:
                    minimal_deck[board] = []
            # Commander cards: include isEtched if present
            for board in ["commander", "displayCommander"]:
                cards_list = deck.get(board)
                if cards_list:
                    minimal_deck[board] = [
                        {
                            k: v
                            for k, v in c.items()
                            if k in ("count", "uuid", "isFoil", "isEtched") and v not in (None, False)
                        }
                        for c in cards_list
                        if isinstance(c, dict)
                    ]
                else:
                    minimal_deck[board] = []
            # Other optional lists (tokens, planes, schemes) - no isEtched
            for board in ["tokens", "planes", "schemes"]:
                cards_list = deck.get(board)
                if cards_list:
                    minimal_deck[board] = [
                        {k: v for k, v in c.items() if k in ("count", "uuid", "isFoil") and v not in (None, False)}
                        for c in cards_list
                        if isinstance(c, dict)
                    ]
                else:
                    minimal_deck[board] = []
            minimal_decks.append(minimal_deck)
        return minimal_decks

    def build_sealed_products(self, set_code: str) -> list[dict[str, Any]] | None:
        """Build sealed product list for a set, or None if no products."""
        if self.ctx.sealed_df is None or len(self.ctx.sealed_df.columns) == 0:
            return None
        set_sealed = self.ctx.sealed_df.filter(pl.col("setCode") == set_code)
        if len(set_sealed) == 0:
            return None
        models = SealedProduct.from_dataframe(set_sealed)
        return [m.to_polars_dict(exclude_none=True) for m in models]

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
        from mtgjson5.constants import BAD_FILE_NAMES

        def _normalize_set_code(dir_code: str) -> str:
            """Convert Windows-safe directory code back to original set code."""
            if dir_code.endswith("_") and dir_code[:-1] in BAD_FILE_NAMES:
                return dir_code[:-1]
            return dir_code

        # Sets with card parquet data
        card_sets = {
            _normalize_set_code(p.name.replace("setCode=", ""))
            for p in self.ctx.parquet_dir.iterdir()
            if p.is_dir() and p.name.startswith("setCode=")
        }

        # Sets with token parquet data
        token_sets: set[str] = set()
        if self.ctx.tokens_dir.exists():
            token_sets = {
                _normalize_set_code(p.name.replace("setCode=", ""))
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
            code
            for code in all_sets
            if not (self.ctx.set_meta.get(code, {}).get("type") == "token" and code.startswith("T"))
        )


def _oracle_key(row: dict[str, Any], cols: tuple[str, ...] = ORACLE_IDENTITY_COLS) -> tuple[Any, ...]:
    """Build a hashable tuple key from a row dict for oracle identity matching.

    Converts list values (like colorIdentity) to tuples for hashability.
    """
    parts: list[Any] = []
    for c in cols:
        v = row.get(c)
        if isinstance(v, list):
            v = tuple(v)
        parts.append(v)
    return tuple(parts)


class AtomicCardsAssembler(Assembler):
    """Assembles AtomicCards grouped by name."""

    @staticmethod
    def _strip_none_recursive(obj: Any) -> Any:
        """Recursively strip None values from dicts and lists.

        Polars struct columns contain None for missing values, but Pydantic
        TypedDicts don't accept None where a string is expected.
        """
        if isinstance(obj, dict):
            return {k: AtomicCardsAssembler._strip_none_recursive(v) for k, v in obj.items() if v is not None}
        if isinstance(obj, list):
            return [AtomicCardsAssembler._strip_none_recursive(v) for v in obj if v is not None]
        return obj

    @staticmethod
    def _build_oracle_lookups(
        rows: list[dict[str, Any]],
    ) -> tuple[dict[tuple[Any, ...], list[dict[str, Any]]], dict[tuple[Any, ...], dict[str, str]]]:
        """Build consolidated foreignData and legalities lookups across all printings.

        Iterates all rows (pre-dedup) and builds:

        foreign_lookup: best foreignData entry per oracle identity + language,
            scored by completeness (non-null faceName, text, type count).
        legalities_lookup: merged legalities keeping first non-null value per format
            (rows are pre-sorted so preferred printings come first).

        Returns:
            (foreign_lookup, legalities_lookup)
        """
        # {oracle_key: {language: (score, entry)}}
        foreign_by_lang: dict[tuple[Any, ...], dict[str, tuple[int, dict[str, Any]]]] = {}
        legalities_lookup: dict[tuple[Any, ...], dict[str, str]] = {}

        for row in rows:
            key = _oracle_key(row)

            # --- foreignData ---
            fd_list = row.get("foreignData")
            if fd_list and isinstance(fd_list, list):
                lang_map = foreign_by_lang.setdefault(key, {})
                for entry in fd_list:
                    if not isinstance(entry, dict):
                        continue
                    lang = entry.get("language")
                    if not lang:
                        continue
                    score = sum(1 for f in ("faceName", "text", "type") if entry.get(f))
                    existing = lang_map.get(lang)
                    if existing is None or score > existing[0]:
                        lang_map[lang] = (score, entry)

            # --- legalities ---
            leg = row.get("legalities")
            if leg and isinstance(leg, dict):
                merged = legalities_lookup.setdefault(key, {})
                for fmt, val in leg.items():
                    if fmt not in merged and val is not None:
                        merged[fmt] = val

        # Flatten foreign_by_lang to {key: [entries]}
        foreign_lookup: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for key, lang_map in foreign_by_lang.items():
            foreign_lookup[key] = [entry for _, entry in lang_map.values()]

        return foreign_lookup, legalities_lookup

    def iter_atomic(self) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        """Iterate over atomic cards grouped by name."""

        atomic_schema = CardAtomic.polars_schema()
        atomic_fields = set(atomic_schema.keys())

        lf = self.load_all_cards()
        available = set(lf.collect_schema().names())
        select_cols = list(atomic_fields & available)

        if "name" not in select_cols:
            select_cols.append("name")

        # Temporarily include printing-level columns for sort preference
        extra_cols = [c for c in EXTRA_SORT_COLS if c in available and c not in select_cols]
        select_with_extras = select_cols + extra_cols

        # Build sort expressions: prefer non-oversized, non-funny, non-promo, side="a",
        # and prefer printings that have actual legalities over those with all-null
        sort_exprs = []
        if "isFunny" in available:
            sort_exprs.append(pl.col("isFunny").fill_null(False))
        for col_name in EXTRA_SORT_COLS:
            if col_name in available:
                sort_exprs.append(pl.col(col_name).fill_null(False))
        if "legalities" in available:
            sort_exprs.append(pl.col("legalities").struct.field("vintage").is_null())
        if "foreignData" in available:
            sort_exprs.append(pl.col("foreignData").list.len().cast(pl.Int32).neg())
        if "side" in available:
            sort_exprs.append(pl.col("side").fill_null(""))

        df = lf.select(select_with_extras)
        if sort_exprs:
            df = df.sort(sort_exprs)

        # Collect ALL printings before dedup so we can consolidate foreignData/legalities
        all_rows_df = df.collect()
        all_rows = all_rows_df.to_dicts()

        # Build consolidated lookups from all printings
        foreign_lookup, legalities_lookup = self._build_oracle_lookups(all_rows)
        del all_rows

        # Now dedup on the collected DataFrame
        deduped = all_rows_df.unique(
            subset=list(ORACLE_IDENTITY_COLS),
            keep="first",
        ).sort(["name", "side"])
        if extra_cols:
            deduped = deduped.drop(extra_cols)

        current_name: str | None = None
        current_cards: list[dict[str, Any]] = []

        for row in deduped.to_dicts():
            name = row.get("name", "")

            if name != current_name:
                if current_name is not None and current_cards:
                    yield current_name, current_cards
                current_name = name
                current_cards = []

            key = _oracle_key(row)

            consolidated_fd = foreign_lookup.get(key)
            if consolidated_fd:
                row["foreignData"] = [
                    {k: v for k, v in entry.items() if k not in ATOMIC_FOREIGN_STRIP}
                    for entry in consolidated_fd
                    if isinstance(entry, dict)
                ]

            merged_leg = legalities_lookup.get(key)
            if merged_leg:
                row["legalities"] = merged_leg

            # Strip printing-specific identifiers
            identifiers = row.get("identifiers")
            if isinstance(identifiers, dict):
                row["identifiers"] = {k: v for k, v in identifiers.items() if k in ATOMIC_IDENTIFIERS}

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


class DeckAssembler(Assembler):
    """Assembles Deck objects with expanded card data."""

    def __init__(self, ctx: AssemblyContext):
        super().__init__(ctx)
        self._uuid_index: dict[str, dict[str, Any]] | None = None
        self._token_uuids: set[str] | None = None

    @property
    def uuid_index(self) -> dict[str, dict[str, Any]]:
        """Lazy-build UUID -> card/token dict index.

        Uses CardSet for cards and CardToken for tokens.
        Deck-specific fields (count, isFoil, isEtched) are added in expand_card_list().

        Note: After building the index, source DataFrames are deleted to free memory.
        """
        if self._uuid_index is None:
            self._token_uuids = set()
            cards_df = self.load_all_cards().collect()
            models = CardSet.from_dataframe(cards_df)
            self._uuid_index = {m.uuid: m.to_polars_dict(exclude_none=True) for m in models}
            del cards_df

            tokens_df = self.load_all_tokens().collect()
            if not tokens_df.is_empty():
                token_models = CardToken.from_dataframe(tokens_df)
                for m in token_models:
                    self._uuid_index[m.uuid] = m.to_polars_dict(exclude_none=True)
                    self._token_uuids.add(m.uuid)
            del tokens_df

        return self._uuid_index

    def is_token(self, uuid: str) -> bool:
        """Check if a UUID belongs to a token."""
        # Ensure index is built first
        _ = self.uuid_index
        return uuid in self._token_uuids  # type: ignore[operator]

    def expand_card_list(self, refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Expand card references to full card/token objects.

        Cards are validated through CardDeck model.
        Tokens use their existing data with deck fields (count, isFoil) added.
        """
        result = []
        for ref in refs:
            uuid = ref.get("uuid")
            if not uuid:
                continue

            card = self.uuid_index.get(uuid)
            if card is None:
                continue

            expanded = dict(card)
            expanded["count"] = ref.get("count", 1)
            expanded["isFoil"] = ref.get("isFoil", False)
            expanded["isEtched"] = ref.get("isEtched", False)

            if self.is_token(uuid):
                # Tokens validated through CardToken model
                # CardToken doesn't have deck fields, so add them after validation
                deck_token = CardToken.from_polars_row(expanded)
                token_dict = deck_token.to_polars_dict(exclude_none=True, keep_empty_lists=True)
                token_dict["count"] = ref.get("count", 1)
                if ref.get("isFoil"):
                    token_dict["isFoil"] = True
                result.append(token_dict)
            else:
                # Cards validated through CardDeck model (includes count/isFoil/isEtched)
                deck_card = CardDeck.from_polars_row(expanded)
                result.append(deck_card.to_polars_dict(exclude_none=True, keep_empty_lists=True))

        return result

    def build(self, deck_data: dict[str, Any]) -> dict[str, Any]:
        """Build a complete Deck with expanded cards."""
        result = {
            "code": deck_data.get("code", deck_data.get("setCode", "")),
            "name": deck_data.get("name", ""),
            "type": deck_data.get("type", ""),
            "releaseDate": deck_data.get("releaseDate"),
        }

        sealed_uuids = deck_data.get("sealedProductUuids")
        result["sealedProductUuids"] = sealed_uuids if sealed_uuids else None

        for board in ["mainBoard", "sideBoard"]:
            refs = deck_data.get(board, [])
            result[board] = self.expand_card_list(refs) if refs else []

        for board in ["commander", "displayCommander", "planes", "schemes", "tokens"]:
            refs = deck_data.get(board)
            result[board] = self.expand_card_list(refs) if refs else []

        result["sourceSetCodes"] = deck_data.get("sourceSetCodes") or []

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
            safe_name = "".join(c for c in name.title() if c.isalnum())

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
        result = [m.to_polars_dict(exclude_none=True) for m in models]

        # Merge token products from assembly context lookup
        if self.ctx.token_products:
            for token_dict in result:
                uuid = token_dict.get("uuid")
                if uuid and uuid in self.ctx.token_products:
                    token_dict["tokenProducts"] = self.ctx.token_products[uuid]

        return result

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
        translations = meta.get("translations", {})

        # Base set structure
        # Use None checks instead of truthy checks since 0 is a valid value for size fields
        base_size = meta.get("baseSetSize")
        total_size = meta.get("totalSetSize")
        set_data: dict[str, Any] = {
            "baseSetSize": (
                base_size if base_size is not None else len([c for c in cards if not c.get("isReprint")]) or len(cards)
            ),
            "cards": cards,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "tokens": tokens,
            "totalSetSize": total_size if total_size is not None else len(cards),
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
        set_data["languages"] = self.build_languages(cards)

        # Booster config
        if include_booster and set_code in self.ctx.booster_configs:
            set_data["booster"] = self.ctx.booster_configs[set_code]

        # Decks (minimal format)
        if include_decks:
            decks = self.build_minimal_decks(set_code)
            if decks is not None:
                set_data["decks"] = decks

        # Sealed products
        if include_sealed:
            sealed = self.build_sealed_products(set_code)
            if sealed is not None:
                set_data["sealedProduct"] = sealed

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
        translations = meta.get("translations", {})
        base_size = meta.get("baseSetSize")
        total_size = meta.get("totalSetSize")

        entry: dict[str, Any] = {
            "baseSetSize": base_size if base_size is not None else card_count,
            "code": set_code,
            "isFoilOnly": meta.get("isFoilOnly", False),
            "isOnlineOnly": meta.get("isOnlineOnly", False),
            "keyruneCode": meta.get("keyruneCode", set_code),
            "name": meta.get("name", set_code),
            "releaseDate": meta.get("releaseDate", ""),
            "totalSetSize": total_size if total_size is not None else card_count,
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

        # Languages from foreign data
        if not df.is_empty():
            cards = [m.to_polars_dict(exclude_none=True) for m in CardSet.from_dataframe(df)]
            entry["languages"] = self.build_languages(cards)
        else:
            entry["languages"] = ["English"]

        # Decks (minimal format)
        decks = self.build_minimal_decks(set_code)
        if decks is not None:
            entry["decks"] = decks

        # Sealed products
        sealed = self.build_sealed_products(set_code)
        if sealed is not None:
            entry["sealedProduct"] = sealed

        return entry

    def build(self) -> list[dict[str, Any]]:
        """Build all SetList entries."""
        return [self.build_one(code) for code in self.iter_set_codes()]


class TcgplayerSkusAssembler(Assembler):
    """Assembles TcgplayerSkus.json - maps UUIDs to TCGPlayer SKU information."""

    def __init__(self, ctx: AssemblyContext):
        super().__init__(ctx)
        self._tcg_skus_lf: pl.LazyFrame | None = None
        self._tcg_to_uuid_lf: pl.LazyFrame | None = None
        self._tcg_etched_to_uuid_lf: pl.LazyFrame | None = None

    def _load_tcg_data(self) -> None:
        """Load TCGPlayer SKU data, awaiting background fetch if needed."""
        from mtgjson5 import constants
        from mtgjson5.v2.data import GLOBAL_CACHE

        GLOBAL_CACHE._await_tcg_skus()

        lazy_cache = constants.CACHE_PATH / "lazy"

        tcg_skus_path = lazy_cache / "tcg_skus.parquet"
        if not tcg_skus_path.exists():
            tcg_skus_path = constants.CACHE_PATH / "tcg_skus.parquet"
        if tcg_skus_path.exists():
            self._tcg_skus_lf = pl.scan_parquet(tcg_skus_path)

        tcg_to_uuid_path = lazy_cache / "tcg_to_uuid.parquet"
        if not tcg_to_uuid_path.exists():
            tcg_to_uuid_path = constants.CACHE_PATH / "tcg_to_uuid.parquet"
        if tcg_to_uuid_path.exists():
            self._tcg_to_uuid_lf = pl.scan_parquet(tcg_to_uuid_path)

        tcg_etched_path = lazy_cache / "tcg_etched_to_uuid.parquet"
        if not tcg_etched_path.exists():
            tcg_etched_path = constants.CACHE_PATH / "tcg_etched_to_uuid.parquet"
        if tcg_etched_path.exists():
            self._tcg_etched_to_uuid_lf = pl.scan_parquet(tcg_etched_path)

    def build(self) -> dict[str, list[dict[str, Any]]]:
        """Build TcgplayerSkus data dict.

        Returns:
            Dict mapping UUID to list of SKU entries.
            Each SKU entry contains: skuId, productId, language, printing, condition,
            and optionally finish (for etched products).
        """
        from mtgjson5.utils import LOGGER
        from mtgjson5.v2.providers.tcgplayer.models import (
            CONDITION_MAP,
            LANGUAGE_MAP,
            PRINTING_MAP,
        )

        self._load_tcg_data()

        if self._tcg_skus_lf is None:
            LOGGER.warning(
                "TCG SKUs data not found (tcg_skus.parquet missing). "
                "TcgplayerSkus.json will be empty. "
                "Run TCGProvider.fetch_all_products() to fetch SKU data."
            )
            return {}

        if self._tcg_to_uuid_lf is None and self._tcg_etched_to_uuid_lf is None:
            LOGGER.warning("TCG to UUID mappings not found, TcgplayerSkus.json will be empty")
            return {}

        tcg_skus_df = self._tcg_skus_lf.collect()

        if "skus" not in tcg_skus_df.columns:
            LOGGER.warning("No 'skus' column in TCG data, TcgplayerSkus.json will be empty")
            return {}

        flattened = (
            tcg_skus_df.explode("skus")
            .unnest("skus")
            .with_columns(
                [
                    pl.col("languageId").replace_strict(LANGUAGE_MAP, default="UNKNOWN").alias("language"),
                    pl.col("printingId")
                    .replace_strict(PRINTING_MAP, default="UNKNOWN")
                    .str.replace("_", " ")
                    .alias("printing"),
                    pl.col("conditionId").replace_strict(CONDITION_MAP, default="UNKNOWN").alias("condition"),
                ]
            )
            .with_columns(
                [
                    pl.col("skuId").cast(pl.Int64),
                    pl.col("productId").cast(pl.Int64),
                ]
            )
        )

        result: dict[str, list[dict[str, Any]]] = {}

        if self._tcg_to_uuid_lf is not None:
            tcg_to_uuid_df = self._tcg_to_uuid_lf.collect()
            if "tcgplayerProductId" in tcg_to_uuid_df.columns and "uuid" in tcg_to_uuid_df.columns:
                tcg_to_uuid_df = tcg_to_uuid_df.with_columns(
                    pl.col("tcgplayerProductId").cast(pl.Int64).alias("productId_join")
                )

                normal_joined = flattened.join(
                    tcg_to_uuid_df.select(["productId_join", "uuid"]),
                    left_on="productId",
                    right_on="productId_join",
                    how="inner",
                )

                self._add_skus_to_result(normal_joined, result, is_etched=False)

        if self._tcg_etched_to_uuid_lf is not None:
            tcg_etched_df = self._tcg_etched_to_uuid_lf.collect()
            if "tcgplayerEtchedProductId" in tcg_etched_df.columns and "uuid" in tcg_etched_df.columns:
                tcg_etched_df = tcg_etched_df.with_columns(
                    pl.col("tcgplayerEtchedProductId").cast(pl.Int64).alias("productId_join")
                )

                etched_joined = flattened.join(
                    tcg_etched_df.select(["productId_join", "uuid"]),
                    left_on="productId",
                    right_on="productId_join",
                    how="inner",
                )

                self._add_skus_to_result(etched_joined, result, is_etched=True)

        # Join sealed product UUIDs
        if self.ctx.sealed_df is not None and not self.ctx.sealed_df.is_empty():
            sealed_df = self.ctx.sealed_df
            if "identifiers" in sealed_df.columns:
                sealed_tcg_map = (
                    sealed_df.select(
                        pl.col("uuid"),
                        pl.col("identifiers").struct.field("tcgplayerProductId").alias("tcgplayerProductId"),
                    )
                    .filter(pl.col("tcgplayerProductId").is_not_null())
                    .with_columns(pl.col("tcgplayerProductId").cast(pl.Int64).alias("productId_join"))
                )
                if len(sealed_tcg_map) > 0:
                    sealed_joined = flattened.join(
                        sealed_tcg_map.select(["productId_join", "uuid"]),
                        left_on="productId",
                        right_on="productId_join",
                        how="inner",
                    )
                    self._add_skus_to_result(sealed_joined, result, is_etched=False)

        LOGGER.info(f"Built TcgplayerSkus with {len(result)} UUIDs")
        return result

    def _add_skus_to_result(
        self,
        joined_df: pl.DataFrame,
        result: dict[str, list[dict[str, Any]]],
        is_etched: bool,
    ) -> None:
        """Add SKUs from joined DataFrame to result dict.

        Args:
            joined_df: DataFrame with SKU data joined to UUIDs
            result: Result dict to populate
            is_etched: Whether these are etched products (adds finish field)
        """
        for row in joined_df.iter_rows(named=True):
            uuid = row.get("uuid")
            if not uuid:
                continue

            sku_entry: dict[str, Any] = {
                "condition": row.get("condition", "UNKNOWN"),
                "language": row.get("language", "UNKNOWN"),
                "printing": row.get("printing", "UNKNOWN"),
                "productId": row.get("productId"),
                "skuId": row.get("skuId"),
            }

            if is_etched:
                sku_entry["finish"] = "ETCHED"

            if uuid not in result:
                result[uuid] = []
            result[uuid].append(sku_entry)


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
        from .serializers import serialize_complex_types

        tables: dict[str, pl.DataFrame] = {}

        # Deduplicate by UUID upfront so all normalized tables are consistent
        cards_df = cards_df.unique(subset=["uuid"])
        schema = cards_df.schema

        # Select card columns, excluding normalized and non-CDN fields
        cards_cols = [c for c in cards_df.columns if c not in CARDS_TABLE_EXCLUDE and not c.startswith("_")]

        cards_for_export = cards_df.select(cards_cols)
        tables["cards"] = serialize_complex_types(cards_for_export)

        # cardIdentifiers - unnest struct
        if "identifiers" in schema and isinstance(schema["identifiers"], pl.Struct):
            tables["cardIdentifiers"] = (
                cards_df.select("uuid", "identifiers").filter(pl.col("identifiers").is_not_null()).unnest("identifiers")
            )

        # cardLegalities - unnest struct
        if "legalities" in schema and isinstance(schema["legalities"], pl.Struct):
            tables["cardLegalities"] = (
                cards_df.select("uuid", "legalities").filter(pl.col("legalities").is_not_null()).unnest("legalities")
            )

        # cardForeignData - explode list of structs
        # Note: foreignData struct may contain its own uuid field, so we alias
        # the card uuid first, then drop any struct uuid after unnest
        if "foreignData" in schema and isinstance(schema["foreignData"], pl.List):
            foreign_df = (
                cards_df.select(pl.col("uuid").alias("_card_uuid"), "foreignData")
                .filter(pl.col("foreignData").list.len() > 0)
                .explode("foreignData")
                .unnest("foreignData")
            )
            # Drop struct's uuid if present, keep card's uuid
            if "uuid" in foreign_df.columns:
                foreign_df = foreign_df.drop("uuid")
            tables["cardForeignData"] = foreign_df.rename({"_card_uuid": "uuid"})

        # cardRulings - explode list of structs
        if "rulings" in schema and isinstance(schema["rulings"], pl.List):
            tables["cardRulings"] = (
                cards_df.select("uuid", "rulings")
                .filter(pl.col("rulings").list.len() > 0)
                .explode("rulings")
                .unnest("rulings")
                .drop("source")
                .rename({"publishedAt": "date", "comment": "text"})
                .with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
            )

        # cardPurchaseUrls - unnest struct
        if "purchaseUrls" in schema and isinstance(schema["purchaseUrls"], pl.Struct):
            tables["cardPurchaseUrls"] = (
                cards_df.select("uuid", "purchaseUrls")
                .filter(pl.col("purchaseUrls").is_not_null())
                .unnest("purchaseUrls")
            )

        # tokens - serialize list columns as JSON strings (like cards)
        if tokens_df is not None and len(tokens_df) > 0:
            token_schema = tokens_df.schema
            token_cols = [c for c in tokens_df.columns if c not in TOKENS_TABLE_EXCLUDE and not c.startswith("_")]
            tokens_for_export = tokens_df.select(token_cols)
            tables["tokens"] = serialize_complex_types(tokens_for_export)

            if "identifiers" in token_schema and isinstance(token_schema["identifiers"], pl.Struct):
                tables["tokenIdentifiers"] = (
                    tokens_df.select("uuid", "identifiers")
                    .filter(pl.col("identifiers").is_not_null())
                    .unnest("identifiers")
                )

        # sets - serialize list columns as JSON strings, exclude translations (normalized)
        if sets_df is not None and len(sets_df) > 0:
            sets_schema = sets_df.schema
            sets_cols = [c for c in sets_df.columns if c not in SETS_TABLE_EXCLUDE and not c.startswith("_")]
            sets_for_export = sets_df.select(sets_cols)
            tables["sets"] = serialize_complex_types(sets_for_export)

            # setTranslations
            if "translations" in sets_schema and isinstance(sets_schema["translations"], pl.Struct):
                trans_wide = (
                    sets_df.select("code", "translations")
                    .filter(pl.col("translations").is_not_null())
                    .unnest("translations")
                )
                lang_cols = [c for c in trans_wide.columns if c != "code"]
                if lang_cols:
                    trans_df = (
                        trans_wide.unpivot(
                            index="code",
                            on=lang_cols,
                            variable_name="language",
                            value_name="translation",
                        )
                        .filter(pl.col("translation").is_not_null())
                        .sort("code", "language")
                    )
                    if len(trans_df) > 0:
                        tables["setTranslations"] = trans_df

        return tables

    @staticmethod
    def build_boosters(
        booster_configs: dict[str, dict[str, Any]],
    ) -> dict[str, pl.DataFrame]:
        """
        Build booster configuration tables.

        Returns:
            Dict with tables: setBoosterSheets, setBoosterSheetCards,
            setBoosterContents, setBoosterContentWeights
        """
        sheets_records: list[dict[str, str | int | bool]] = []
        sheet_cards_records: list[dict[str, str | int]] = []
        contents_records: list[dict[str, str | int]] = []
        weights_records: list[dict[str, str | int]] = []

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

                    sheets_records.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "sheetName": sheet_name,
                            "sheetIsFoil": sheet_data.get("foil", False),
                            "sheetHasBalanceColors": sheet_data.get("balanceColors", False),
                            "sheetTotalWeight": sheet_data.get("totalWeight", 0),
                        }
                    )

                    # Parse cards in sheet
                    cards = sheet_data.get("cards", {})
                    for card_uuid, weight in cards.items():
                        sheet_cards_records.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "sheetName": sheet_name,
                                "cardUuid": card_uuid,
                                "cardWeight": weight,
                            }
                        )

                # Parse boosters (contents and weights)
                boosters = booster_data.get("boosters", [])
                for idx, booster_variant in enumerate(boosters):
                    if not isinstance(booster_variant, dict):
                        continue

                    booster_weight = booster_variant.get("weight", 1)
                    weights_records.append(
                        {
                            "setCode": set_code,
                            "boosterName": booster_name,
                            "boosterIndex": idx,
                            "boosterWeight": booster_weight,
                        }
                    )

                    contents = booster_variant.get("contents", {})
                    for sheet_name, picks in contents.items():
                        contents_records.append(
                            {
                                "setCode": set_code,
                                "boosterName": booster_name,
                                "boosterIndex": idx,
                                "sheetName": sheet_name,
                                "sheetPicks": picks,
                            }
                        )

        return {
            "setBoosterSheets": (pl.DataFrame(sheets_records) if sheets_records else pl.DataFrame()),
            "setBoosterSheetCards": (pl.DataFrame(sheet_cards_records) if sheet_cards_records else pl.DataFrame()),
            "setBoosterContents": (pl.DataFrame(contents_records) if contents_records else pl.DataFrame()),
            "setBoosterContentWeights": (pl.DataFrame(weights_records) if weights_records else pl.DataFrame()),
        }


class KeywordsAssembler(Assembler):
    """Assembles Keywords.json from cached Scryfall catalogs.

    Uses keyword data loaded by GlobalCache during initialization,
    avoiding API calls during assembly.
    """

    def build(self) -> dict[str, list[str]]:
        """Build Keywords data dict.

        Returns:
            Dict with abilityWords, keywordAbilities, and keywordActions lists.
        """
        return self.ctx.keyword_data


class CardTypesAssembler(Assembler):
    """Assembles CardTypes.json from cached Scryfall catalogs and Magic rules.

    Uses card type data loaded by GlobalCache during initialization,
    avoiding API calls during assembly.
    """

    def build(self) -> dict[str, dict[str, list[str]]]:
        """Build CardTypes data dict.

        Returns:
            Dict mapping card types to their subTypes and superTypes.
        """
        cached = self.ctx.card_type_data
        super_types = self.ctx.super_types
        planar_types = self.ctx.planar_types

        subtypes = {
            "artifact": cached.get("artifact", []),
            "battle": cached.get("battle", []),
            "conspiracy": [],
            "creature": cached.get("creature", []),
            "enchantment": cached.get("enchantment", []),
            "instant": cached.get("spell", []),
            "land": cached.get("land", []),
            "phenomenon": [],
            "plane": planar_types,
            "planeswalker": cached.get("planeswalker", []),
            "scheme": [],
            "sorcery": cached.get("spell", []),
            "tribal": [],
            "vanguard": [],
        }

        return {
            card_type: {
                "subTypes": sorted(sub_list),
                "superTypes": sorted(super_types),
            }
            for card_type, sub_list in subtypes.items()
        }


class AllIdentifiersAssembler(Assembler):
    """Assembles AllIdentifiers.json - UUID to card/token mapping.

    Uses chunked processing to minimize memory usage when building
    the UUID -> card/token mapping.
    """

    CHUNK_SIZE = 5000

    def iter_entries(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield (uuid, data) pairs for all cards and tokens.

        Memory-efficient iterator that processes data in chunks.
        """
        from mtgjson5.utils import LOGGER

        seen_uuids: set[str] = set()

        LOGGER.info("Loading all cards for AllIdentifiers...")
        cards_lf = self.load_all_cards()
        cards_df = cards_lf.collect()

        for start in range(0, len(cards_df), self.CHUNK_SIZE):
            chunk = cards_df.slice(start, self.CHUNK_SIZE)
            models = CardSet.from_dataframe(chunk)
            for model in models:
                uuid = model.uuid
                if uuid in seen_uuids:
                    continue
                seen_uuids.add(uuid)
                yield uuid, model.to_polars_dict(exclude_none=True)
            del models, chunk

        del cards_df

        LOGGER.info("Loading all tokens for AllIdentifiers...")
        tokens_lf = self.load_all_tokens()
        tokens_df = tokens_lf.collect()

        if not tokens_df.is_empty():
            for start in range(0, len(tokens_df), self.CHUNK_SIZE):
                chunk = tokens_df.slice(start, self.CHUNK_SIZE)
                token_models = CardToken.from_dataframe(chunk)
                for token in token_models:
                    uuid = token.uuid
                    if uuid in seen_uuids:
                        continue
                    seen_uuids.add(uuid)
                    yield uuid, token.to_polars_dict(exclude_none=True)
                del token_models, chunk

        del tokens_df

        LOGGER.info(f"Built AllIdentifiers with {len(seen_uuids)} entries")

    def build(self) -> dict[str, dict[str, Any]]:
        """Build AllIdentifiers data dict.

        Returns:
            Dict mapping UUID to full card/token data.
        """
        return dict(self.iter_entries())


class EnumValuesAssembler(Assembler):
    """Assembles EnumValues.json by collecting unique values from card/set data.

    Mirrors the v1 MtgjsonEnumValuesObject logic using Polars DataFrames
    instead of iterating over AllPrintings JSON.
    """

    CARD_FIELDS = [
        "availability",
        "boosterTypes",
        "borderColor",
        "colorIdentity",
        "colorIndicator",
        "colors",
        "duelDeck",
        "finishes",
        "frameEffects",
        "frameVersion",
        "language",
        "layout",
        "promoTypes",
        "rarity",
        "securityStamp",
        "side",
        "subtypes",
        "supertypes",
        "types",
        "watermark",
    ]

    SET_FIELDS = ["type"]

    FOREIGN_DATA_FIELDS = ["language"]

    def _collect_unique(self, series: pl.Series) -> list[str]:
        """Collect unique non-null string values from a Series, handling lists."""
        if series.dtype == pl.List(pl.String) or isinstance(series.dtype, pl.List):
            exploded = series.explode().drop_nulls().cast(pl.String)
            return sorted(set(exploded.to_list()))
        return sorted(set(series.drop_nulls().cast(pl.String).to_list()))

    def build(self) -> dict[str, dict[str, list[str]]]:
        """Build EnumValues data dict.

        Returns:
            Dict with card, set, foreignData, sealedProduct, deck,
            keywords, and tcgplayerSkus enum value lists.
        """
        from mtgjson5.utils import LOGGER

        result: dict[str, dict[str, list[str]]] = {}

        # --- Card enums ---
        LOGGER.info("EnumValues: collecting card enums...")
        cards_lf = self.load_all_cards()
        tokens_lf = self.load_all_tokens()

        # Combine cards and tokens
        card_cols = [c for c in self.CARD_FIELDS if c in cards_lf.collect_schema().names()]
        token_cols = [c for c in self.CARD_FIELDS if c in tokens_lf.collect_schema().names()]
        all_cols = sorted(set(card_cols) | set(token_cols))

        cards_df = cards_lf.select([c for c in all_cols if c in cards_lf.collect_schema().names()]).collect()
        tokens_df = tokens_lf.select([c for c in all_cols if c in tokens_lf.collect_schema().names()]).collect()

        card_enums: dict[str, list[str]] = {}
        for col in all_cols:
            values: set[str] = set()
            if col in cards_df.columns:
                values.update(self._collect_unique(cards_df[col]))
            if col in tokens_df.columns:
                values.update(self._collect_unique(tokens_df[col]))
            card_enums[col] = sorted(values)

        result["card"] = card_enums

        # --- Set enums ---
        set_enums: dict[str, list[str]] = {}
        for field in self.SET_FIELDS:
            values_set: set[str] = set()
            for meta in self.ctx.set_meta.values():
                val = meta.get(field)
                if val is not None:
                    values_set.add(str(val))
            set_enums[field] = sorted(values_set)

        # Collect set languages from foreignData
        languages: set[str] = {"English"}
        fd_col = "foreignData"
        if fd_col in cards_lf.collect_schema().names():
            fd_series = cards_lf.select(fd_col).collect()[fd_col]
            if isinstance(fd_series.dtype, pl.List):
                exploded = fd_series.explode().drop_nulls()
                if isinstance(exploded.dtype, pl.Struct) and "language" in [f.name for f in exploded.dtype.fields]:
                    lang_series = exploded.struct.field("language").drop_nulls()
                    languages.update(lang_series.to_list())
        set_enums["languages"] = sorted(languages)
        result["set"] = set_enums

        # --- ForeignData enums ---
        foreign_enums: dict[str, list[str]] = {}
        foreign_languages = languages - {"English"}
        foreign_enums["language"] = sorted(foreign_languages)
        result["foreignData"] = foreign_enums

        # --- Sealed product enums ---
        if self.ctx.sealed_df is not None and not self.ctx.sealed_df.is_empty():
            sealed_enums: dict[str, list[str]] = {}
            for field in ("category", "subtype"):
                if field in self.ctx.sealed_df.columns:
                    sealed_enums[field] = self._collect_unique(self.ctx.sealed_df[field])
            if sealed_enums:
                result["sealedProduct"] = sealed_enums

        # --- Deck enums ---
        if self.ctx.decks_df is not None and not self.ctx.decks_df.is_empty():
            deck_enums: dict[str, list[str]] = {}
            if "type" in self.ctx.decks_df.columns:
                deck_enums["type"] = self._collect_unique(self.ctx.decks_df["type"])
            if deck_enums:
                result["deck"] = deck_enums

        # --- Keywords ---
        if self.ctx.keyword_data:
            result["keywords"] = {k: sorted(v) for k, v in self.ctx.keyword_data.items()}

        # --- TCGPlayer SKU enums ---
        from mtgjson5.v2.providers.tcgplayer.models import (
            CONDITION_MAP,
            LANGUAGE_MAP,
            PRINTING_MAP,
        )

        result["tcgplayerSkus"] = {
            "condition": sorted(CONDITION_MAP.values()),
            "finishes": ["FOIL_ETCHED"],
            "language": sorted(LANGUAGE_MAP.values()),
            "printing": sorted(PRINTING_MAP.values()),
        }

        return result


class CompiledListAssembler:
    """Assembles CompiledList.json - sorted list of compiled output files."""

    COMPILED_FILES = [
        "AllIdentifiers",
        "AllPrices",
        "AllPricesToday",
        "AllPrintings",
        "AtomicCards",
        "CardTypes",
        "CompiledList",
        "DeckList",
        "EnumValues",
        "Keywords",
        "Legacy",
        "LegacyAtomic",
        "Meta",
        "Modern",
        "ModernAtomic",
        "PauperAtomic",
        "Pioneer",
        "PioneerAtomic",
        "SetList",
        "Standard",
        "StandardAtomic",
        "TcgplayerSkus",
        "Vintage",
        "VintageAtomic",
    ]

    def build(self) -> list[str]:
        """Build CompiledList data.

        Returns:
            Sorted list of compiled output file names.
        """
        return sorted(self.COMPILED_FILES)
