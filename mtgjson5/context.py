"""
Pipeline context for MTGJSON card building.

Provides a container for all lookup data needed by the card pipeline
"""

from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid5

import polars as pl


if TYPE_CHECKING:
    from mtgjson5.categoricals import DynamicCategoricals

from mtgjson5.constants import LANGUAGE_MAP
from mtgjson5.mtgjson_models import (
    CardAtomic,
    CardDeck,
    CardSet,
    CardToken,
)
from mtgjson5.utils import LOGGER, get_expanded_set_codes


# UUID namespace for MTGJSON
_DNS_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

@dataclass
class PipelineContext:
    """
    Container for all lookup data needed by the card pipeline.

    Allows pipeline functions to be tested with smaller, controlled datasets
    rather than always pulling from GLOBAL_CACHE.

    After calling consolidate_lookups(), data is organized by join key for
    efficient streaming pipeline execution.
    """

    args: Namespace | None = None
    scryfall_id_filter: set[str] | None = None
    final_cards_lf: pl.LazyFrame | None = None

    cards_lf: pl.LazyFrame | None = None
    raw_rulings_lf: pl.LazyFrame | None = None
    sets_df: pl.LazyFrame | pl.DataFrame | None = None

    card_kingdom_df: pl.DataFrame | pl.LazyFrame | None = None
    card_kingdom_raw_df: pl.DataFrame | pl.LazyFrame | None = None
    mcm_lookup_df: pl.DataFrame | pl.LazyFrame | None = None
    salt_df: pl.DataFrame | pl.LazyFrame | None = None
    spellbook_df: pl.DataFrame | pl.LazyFrame | None = None
    sld_subsets_df: pl.DataFrame | pl.LazyFrame | None = None
    uuid_cache_df: pl.DataFrame | pl.LazyFrame | None = None
    orientation_df: pl.DataFrame | pl.LazyFrame | None = None
    gatherer_df: pl.DataFrame | pl.LazyFrame | None = None

    rulings_df: pl.DataFrame | pl.LazyFrame | None = None
    foreign_data_df: pl.DataFrame | pl.LazyFrame | None = None
    uuid_lookup_df: pl.DataFrame | None = None

    identifiers_lf: pl.LazyFrame | None = None
    oracle_data_lf: pl.LazyFrame | None = None
    set_number_lf: pl.LazyFrame | None = None
    name_lf: pl.LazyFrame | None = None
    multiverse_bridge_lf: pl.LazyFrame | None = None
    signatures_lf: pl.LazyFrame | None = None

    sealed_cards_lf: pl.LazyFrame | None = None
    sealed_products_lf: pl.LazyFrame | None = None
    sealed_contents_lf: pl.LazyFrame | None = None
    decks_lf: pl.LazyFrame | None = None
    boosters_lf: pl.LazyFrame | None = None

    card_to_products_df: pl.DataFrame | pl.LazyFrame | None = None

    tcg_skus_lf: pl.LazyFrame | None = None
    tcg_sku_map_df: pl.DataFrame | None = None
    tcg_to_uuid_df: pl.DataFrame | None = None
    tcg_etched_to_uuid_df: pl.DataFrame | None = None
    mtgo_to_uuid_df: pl.DataFrame | None = None
    cardmarket_to_uuid_df: pl.DataFrame | None = None
    uuid_to_oracle_df: pl.DataFrame | None = None

    meld_triplets: dict = field(default_factory=dict)
    manual_overrides: dict = field(default_factory=dict)
    multiverse_bridge_cards: dict = field(default_factory=dict)
    multiverse_bridge_sets: dict = field(default_factory=dict)
    gatherer_map: dict = field(default_factory=dict)

    standard_legal_sets: set[str] = field(default_factory=set)
    unlimited_cards: set[str] = field(default_factory=set)

    # Resource path for JSON lookups
    resource_path: Path | None = None

    # Model types for schema generation (use Model.polars_schema() when needed)
    card_set_model: type = field(default=CardSet)
    card_token_model: type = field(default=CardToken)
    card_deck_model: type = field(default=CardDeck)
    card_atomic_model: type = field(default=CardAtomic)

    # Categoricals
    categoricals: DynamicCategoricals | None = None

    # Property aliases for _lf -> _df naming (functions expect _df suffix)
    @property
    def decks_df(self) -> pl.LazyFrame | None:
        """Alias for decks_lf (backward compat)."""
        return self.decks_lf

    @property
    def sealed_products_df(self) -> pl.LazyFrame | None:
        """Alias for sealed_products_lf (backward compat)."""
        return self.sealed_products_lf

    @property
    def sealed_contents_df(self) -> pl.LazyFrame | None:
        """Alias for sealed_contents_lf (backward compat)."""
        return self.sealed_contents_lf

    @property
    def boosters_df(self) -> pl.LazyFrame | None:
        """Alias for boosters_lf (backward compat)."""
        return self.boosters_lf

    @property
    def pretty(self) -> bool:
        """Whether to pretty-print JSON output."""
        return getattr(self.args, "pretty", False) if self.args else False

    @property
    def output_types(self) -> bool:
        """Whether a specific output type is requested."""
        if not self.args:
            return False
        outputs = {o.lower() for o in (getattr(self.args, "outputs", None) or [])}
        return outputs == {""}

    @property
    def include_referrals(self) -> bool:
        """Whether to include referral links."""
        return getattr(self.args, "referrals", False) if self.args else False

    @property
    def all_sets(self) -> bool:
        """Whether to build all sets."""
        return getattr(self.args, "all_sets", False) if self.args else False

    @property
    def export_formats(self) -> set[str] | None:
        """Set of export formats requested."""
        if not self.args:
            return None
        formats_raw = getattr(self.args, "export", None)
        if formats_raw and isinstance(formats_raw, list | tuple | set):
            # pylint: disable-next=not-an-iterable
            return {f.lower() for f in formats_raw}
        return None

    @property
    def sets_to_build(self) -> set[str] | None:
        """Set of set codes to build."""
        if not self.args:
            return None
        arg_sets = getattr(self.args, "sets", None)
        sets = get_expanded_set_codes(arg_sets)
        return {s.upper() for s in sets} if sets else None

    @classmethod
    def from_global_cache(cls, args: Namespace | None = None) -> PipelineContext:
        """
        Create a PipelineContext from the global cache.

        All DataFrames are LazyFrames after GlobalCache._dump_and_reload_as_lazy().
        """
        from mtgjson5 import constants
        from mtgjson5.cache import GLOBAL_CACHE
        from mtgjson5.categoricals import discover_categoricals

        # Discover categoricals from the raw cards data
        categoricals = None
        if GLOBAL_CACHE.cards_df is not None:
            categoricals = discover_categoricals(
                GLOBAL_CACHE.cards_df,
                GLOBAL_CACHE.sets_df,
            )

        ctx = cls(
            args=args,
            # Core data
            cards_lf=GLOBAL_CACHE.cards_df,
            raw_rulings_lf=GLOBAL_CACHE.raw_rulings_df,
            sets_df=GLOBAL_CACHE.sets_df,
            # Provider lookups
            card_kingdom_df=GLOBAL_CACHE.card_kingdom_df,
            card_kingdom_raw_df=GLOBAL_CACHE.card_kingdom_raw_df,
            mcm_lookup_df=GLOBAL_CACHE.mcm_lookup_df,
            salt_df=GLOBAL_CACHE.salt_df,
            spellbook_df=GLOBAL_CACHE.spellbook_df,
            sld_subsets_df=GLOBAL_CACHE.sld_subsets_df,
            uuid_cache_df=GLOBAL_CACHE.uuid_cache_df,
            orientation_df=GLOBAL_CACHE.orientation_df,
            gatherer_df=GLOBAL_CACHE.gatherer_df,
            # Pre-computed
            rulings_df=GLOBAL_CACHE.rulings_df,
            foreign_data_df=GLOBAL_CACHE.foreign_data_df,
            # GitHub data
            sealed_cards_lf=GLOBAL_CACHE.sealed_cards_df,
            sealed_products_lf=GLOBAL_CACHE.sealed_products_df,
            sealed_contents_lf=GLOBAL_CACHE.sealed_contents_df,
            decks_lf=GLOBAL_CACHE.decks_df,
            boosters_lf=GLOBAL_CACHE.boosters_df,
            card_to_products_df=GLOBAL_CACHE.sealed_cards_df,  # Alias for sourceProducts
            # TCG data
            tcg_skus_lf=GLOBAL_CACHE.tcg_skus_lf,
            tcg_sku_map_df=GLOBAL_CACHE.tcg_sku_map_df,
            tcg_to_uuid_df=GLOBAL_CACHE.tcg_to_uuid_df,
            tcg_etched_to_uuid_df=GLOBAL_CACHE.tcg_etched_to_uuid_df,
            mtgo_to_uuid_df=GLOBAL_CACHE.mtgo_to_uuid_df,
            cardmarket_to_uuid_df=GLOBAL_CACHE.cardmarket_to_uuid_df,
            uuid_to_oracle_df=GLOBAL_CACHE.uuid_to_oracle_df,
            # Dict lookups
            meld_triplets=GLOBAL_CACHE.meld_triplets or {},
            manual_overrides=GLOBAL_CACHE.manual_overrides or {},
            multiverse_bridge_cards=GLOBAL_CACHE.multiverse_bridge_cards or {},
            multiverse_bridge_sets=GLOBAL_CACHE.multiverse_bridge_sets or {},
            gatherer_map=GLOBAL_CACHE.gatherer_map or {},
            # Sets
            standard_legal_sets=GLOBAL_CACHE.standard_legal_sets or set(),
            unlimited_cards=GLOBAL_CACHE.unlimited_cards or set(),
            # Resource path
            resource_path=constants.RESOURCE_PATH,
            # Categoricals
            categoricals=categoricals,
            scryfall_id_filter=None,
        )

        return ctx

    def consolidate_lookups(self) -> PipelineContext:
        """
        Consolidate separate lookup tables into combined tables by join key.
        """
        LOGGER.info("Consolidating lookup tables...")

        self._build_identifiers_lookup()
        self._build_oracle_data_lookup()
        self._build_set_number_lookup()
        self._build_name_lookup()
        self._build_multiverse_bridge_lookup()
        self._build_signatures_lookup()

        return self

    def _build_identifiers_lookup(self) -> None:
        """
        Build consolidated identifiers lookup (by scryfallId + side).
        """
        # Start with uuid_cache as base
        if self.uuid_cache_df is None:
            LOGGER.info("  identifiers: No uuid_cache_df, skipping")
            return

        uuid_cache_raw = self.uuid_cache_df
        if isinstance(uuid_cache_raw, pl.LazyFrame):
            uuid_cache: pl.DataFrame = uuid_cache_raw.collect()
        else:
            uuid_cache = uuid_cache_raw

        if uuid_cache.height == 0:
            LOGGER.info("  identifiers: uuid_cache_df is empty, skipping")
            return

        result: pl.DataFrame = uuid_cache.select(["scryfallId", "side", "cachedUuid"])
        LOGGER.info(f"  identifiers: +uuid_cache ({result.height:,} rows)")

        # Add Card Kingdom data (by scryfallId only, duplicated for all sides)
        if self.card_kingdom_df is not None:
            ck_raw = self.card_kingdom_df
            if isinstance(ck_raw, pl.LazyFrame):
                ck: pl.DataFrame = ck_raw.collect()
            else:
                ck = ck_raw
            if ck.height > 0:
                # Rename 'id' to 'scryfallId' for join
                ck = ck.rename({"id": "scryfallId"}).select(
                    [
                        "scryfallId",
                        "cardKingdomId",
                        "cardKingdomFoilId",
                        "cardKingdomUrl",
                        "cardKingdomFoilUrl",
                    ]
                )
                result = result.join(ck, on="scryfallId", how="left")
                LOGGER.info(f"  identifiers: +card_kingdom ({ck.height:,} rows)")

        # Add orientation data (by scryfallId only)
        if self.orientation_df is not None:
            orient_raw = self.orientation_df
            if isinstance(orient_raw, pl.LazyFrame):
                orient: pl.DataFrame = orient_raw.collect()
            else:
                orient = orient_raw
            if orient.height > 0:
                result = result.join(orient, on="scryfallId", how="left")
                LOGGER.info(f"  identifiers: +orientation ({orient.height:,} rows)")

        self.identifiers_lf = result.lazy()
        LOGGER.info(
            f"  identifiers_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _build_oracle_data_lookup(self) -> None:
        """
        Build consolidated oracle data lookup (by oracleId).
        """

        frames: list[tuple[str, pl.DataFrame]] = []

        # Salt data
        if self.salt_df is not None:
            salt_raw = self.salt_df
            if isinstance(salt_raw, pl.LazyFrame):
                salt: pl.DataFrame = salt_raw.collect()
            else:
                salt = salt_raw
            if salt.height > 0:
                salt = salt.select(
                    [
                        "oracleId",
                        pl.col("edhrecSaltiness").round(2),
                        "edhrecRank",
                    ]
                )
                frames.append(("salt", salt))
                LOGGER.info(f"  oracle_data: +salt ({salt.height:,} rows)")

        # Rulings (aggregate by oracleId into list of structs)
        if self.raw_rulings_lf is not None:
            rulings_raw = self.raw_rulings_lf
            if isinstance(rulings_raw, pl.LazyFrame):
                rulings: pl.DataFrame = rulings_raw.collect()
            else:
                rulings = rulings_raw
            if rulings.height > 0:
                # Sort by date desc before aggregating
                rulings_agg = (
                    rulings.sort("publishedAt", descending=True)
                    .group_by("oracleId")
                    .agg(
                        pl.struct(["source", "publishedAt", "comment"]).alias("rulings")
                    )
                )
                frames.append(("rulings", rulings_agg))
                LOGGER.info(f"  oracle_data: +rulings ({rulings_agg.height:,} rows)")

        # Printings + originalReleaseDate (computed from cards_lf)
        if self.cards_lf is not None:
            cards_raw = self.cards_lf
            if isinstance(cards_raw, pl.LazyFrame):
                cards: pl.DataFrame = cards_raw.collect()
            else:
                cards = cards_raw

            # Compute printings: oracleId -> list of unique setCodes
            printings = (
                cards.select(["oracleId", "set"])
                .filter(pl.col("oracleId").is_not_null())
                .group_by("oracleId")
                .agg(
                    pl.col("set").str.to_uppercase().unique().sort().alias("printings")
                )
            )

            if printings.height > 0:
                # Get set release dates
                sets_df_raw = self.sets_df
                if sets_df_raw is None:
                    sets_df = pl.DataFrame()
                elif isinstance(sets_df_raw, pl.LazyFrame):
                    sets_df = sets_df_raw.collect()
                else:
                    sets_df = sets_df_raw

                if sets_df.height > 0:
                    # Build set_code -> release_date lookup
                    set_dates = sets_df.select(
                        [
                            pl.col("code").str.to_uppercase().alias("setCode"),
                            pl.col("releasedAt").alias("releaseDate"),
                        ]
                    )

                    # Explode printings, join with dates, find min, re-aggregate
                    printings_with_dates = (
                        printings.explode("printings")
                        .join(
                            set_dates,
                            left_on="printings",
                            right_on="setCode",
                            how="left",
                        )
                        .group_by("oracleId")
                        .agg(
                            [
                                pl.col("printings").sort().alias("printings"),
                                pl.col("releaseDate")
                                .min()
                                .alias("originalReleaseDate"),
                            ]
                        )
                    )
                    frames.append(("printings", printings_with_dates))
                    LOGGER.info(
                        f"  oracle_data: +printings ({printings_with_dates.height:,} rows)"
                    )
                else:
                    # No set dates available, just use printings
                    frames.append(("printings", printings))
                    LOGGER.info(
                        f"  oracle_data: +printings ({printings.height:,} rows, no dates)"
                    )

        if not frames:
            LOGGER.info("  oracle_data: No data to consolidate")
            return

        # Join all frames on oracleId
        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(df, on="oracleId", how="full", coalesce=True)

        self.oracle_data_lf = result.lazy()
        LOGGER.info(
            f"  oracle_data_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _build_set_number_lookup(self) -> None:
        """
        Build consolidated setCode+number lookup.
        """
        frames: list[tuple[str, pl.DataFrame]] = []

        # Foreign data: aggregate non-English cards by set+number
        if self.cards_lf is not None:
            cards_raw = self.cards_lf
            if isinstance(cards_raw, pl.LazyFrame):
                cards: pl.DataFrame = cards_raw.collect()
            else:
                cards = cards_raw

            # Build English card lookup for UUID generation
            # foreignData UUID uses ENGLISH card's scryfallId, not the foreign card's
            # Note: Scryfall doesn't have a 'side' field - we use "a" as default
            # since the UUID formula uses side = "a" for single-faced cards
            english_lookup = (
                cards.filter(pl.col("lang") == "en")
                .with_columns(pl.col("set").str.to_uppercase().alias("setCode"))
                .select([
                    "setCode",
                    pl.col("collectorNumber").alias("number"),
                    pl.col("id").alias("_english_scryfall_id"),
                    # Default to "a" since Scryfall doesn't expose side directly
                    pl.lit("a").alias("_english_side"),
                ])
            )

            # Filter to non-English and aggregate
            foreign_df = (
                cards.filter(pl.col("lang") != "en")
                .with_columns(pl.col("set").str.to_uppercase().alias("setCode"))
                # Join with English cards to get English scryfallId for UUID
                .join(
                    english_lookup,
                    left_on=["setCode", "collectorNumber"],
                    right_on=["setCode", "number"],
                    how="left",
                )
                .with_columns(
                    [
                        # Map language code to full name
                        pl.col("lang")
                        .replace_strict(
                            LANGUAGE_MAP,
                            default=pl.col("lang"),
                        )
                        .alias("language"),
                        # Build full name from card_faces for multi-face
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.col("cardFaces")
                            .list.eval(
                                pl.coalesce(
                                    pl.element().struct.field("printed_name"),
                                    pl.element().struct.field("name"),
                                )
                            )
                            .list.join(" // ")
                        )
                        .otherwise(pl.coalesce("printedName", "name"))
                        .alias("_foreign_name"),
                        # faceName for multi-face cards
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.coalesce(
                                pl.col("cardFaces")
                                .list.first()
                                .struct.field("printed_name"),
                                pl.col("cardFaces").list.first().struct.field("name"),
                            )
                        )
                        .otherwise(None)
                        .alias("_face_name"),
                        # flavorText for foreign cards (from card or first face)
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.col("cardFaces")
                            .list.first()
                            .struct.field("flavor_text")
                        )
                        .otherwise(pl.col("flavorText"))
                        .alias("_flavor_text"),
                        # Generate UUID for foreign data: uuid5(english_scryfallId + side + "_" + language)
                        # Formula: english_scryfall_id + side + "_" + full_language_name
                        pl.concat_str(
                            [
                                pl.col("_english_scryfall_id"),
                                pl.col("_english_side"),
                                pl.lit("_"),
                                pl.col("lang")
                                .replace_strict(
                                    LANGUAGE_MAP,
                                    default=pl.col("lang"),
                                ),
                            ]
                        ).alias("_uuid_source"),
                    ]
                )
                # Generate UUID5 from the source string
                .with_columns(
                    pl.col("_uuid_source")
                    .map_elements(
                        lambda x: str(uuid5(_DNS_NAMESPACE, x)) if x else None,
                        return_dtype=pl.String,
                    )
                    .alias("_foreign_uuid")
                )
                .sort("setCode", "collectorNumber", "language", nulls_last=True)
                .group_by(["setCode", pl.col("collectorNumber").alias("number")])
                .agg(
                    pl.struct(
                        [
                            pl.col("_face_name").alias("faceName"),
                            pl.col("_flavor_text").alias("flavorText"),
                            pl.struct(
                                [
                                    pl.col("multiverseIds")
                                    .list.first()
                                    .cast(pl.String)
                                    .alias("multiverseId"),
                                    pl.col("id").alias("scryfallId"),
                                ]
                            ).alias("identifiers"),
                            pl.col("language"),
                            # multiverseId at top level (deprecated but still present)
                            pl.col("multiverseIds")
                            .list.first()
                            .alias("multiverseId"),
                            pl.col("_foreign_name").alias("name"),
                            pl.col("printedText").alias("text"),
                            pl.col("printedTypeLine").alias("type"),
                            pl.col("_foreign_uuid").alias("uuid"),
                        ]
                    ).alias("foreignData")
                )
            )

            if foreign_df.height > 0:
                frames.append(("foreign", foreign_df))
                LOGGER.info(f"  set_number: +foreign ({foreign_df.height:,} rows)")

        # Duel deck sides from JSON
        duel_deck_df = self._load_duel_deck_sides()
        if duel_deck_df is not None and duel_deck_df.height > 0:
            frames.append(("duel_deck", duel_deck_df))
            LOGGER.info(f"  set_number: +duel_deck ({duel_deck_df.height:,} rows)")

        if not frames:
            LOGGER.info("  set_number: No data to consolidate")
            return

        # Join all frames on setCode + number
        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(
                df, on=["setCode", "number"], how="full", coalesce=True
            )

        self.set_number_lf = result.lazy()
        LOGGER.info(
            f"  set_number_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _load_duel_deck_sides(self) -> pl.DataFrame | None:
        """Load duel deck sides from resource JSON."""
        if self.resource_path is None:
            return None

        sides_path = self.resource_path / "duel_deck_sides.json"
        if not sides_path.exists():
            return None

        with sides_path.open(encoding="utf-8") as f:
            all_sides = json.load(f)

        records = []
        for set_code, number_map in all_sides.items():
            for number, side in number_map.items():
                records.append(
                    {
                        "setCode": set_code,
                        "number": number,
                        "duelDeck": side,
                    }
                )

        return pl.DataFrame(records) if records else None

    def _build_name_lookup(self) -> None:
        """
        Build consolidated name lookup.
        """
        frames: list[tuple[str, pl.DataFrame]] = []

        # Spellbook data
        if self.spellbook_df is not None:
            spellbook_raw = self.spellbook_df
            if isinstance(spellbook_raw, pl.LazyFrame):
                spellbook: pl.DataFrame = spellbook_raw.collect()
            else:
                spellbook = spellbook_raw
            if spellbook.height > 0:
                frames.append(("spellbook", spellbook))
                LOGGER.info(f"  name: +spellbook ({spellbook.height:,} rows)")

        # Meld triplets (convert dict to DataFrame)
        if self.meld_triplets:
            meld_records = [
                {"name": name, "cardParts": parts}
                for name, parts in self.meld_triplets.items()
            ]
            meld_df = pl.DataFrame(meld_records)
            frames.append(("meld", meld_df))
            LOGGER.info(f"  name: +meld ({meld_df.height:,} rows)")

        if not frames:
            LOGGER.info("  name: No data to consolidate")
            return

        # Join all frames on name
        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(df, on="name", how="full", coalesce=True)

        self.name_lf = result.lazy()
        LOGGER.info(f"  name_lf: {result.height:,} rows x {len(result.columns)} cols")

    def _build_multiverse_bridge_lookup(self) -> None:
        """
        Build MultiverseBridge lookup (by scryfallId).
        """
        if not self.multiverse_bridge_cards:
            LOGGER.info("  multiverse_bridge: No data to consolidate")
            return

        records = []
        for scryfall_id, entries in self.multiverse_bridge_cards.items():
            cs_id = None
            cs_foil_id = None
            deckbox_id = None

            for entry in entries:
                if entry.get("is_foil"):
                    cs_foil_id = (
                        str(entry.get("cs_id", "")) if entry.get("cs_id") else None
                    )
                else:
                    cs_id = str(entry.get("cs_id", "")) if entry.get("cs_id") else None
                # deckbox_id is the same for foil/non-foil
                if entry.get("deckbox_id") and not deckbox_id:
                    deckbox_id = str(entry["deckbox_id"])

            records.append(
                {
                    "scryfallId": scryfall_id,
                    "cardsphereId": cs_id,
                    "cardsphereFoilId": cs_foil_id,
                    "deckboxId": deckbox_id,
                }
            )

        result = pl.DataFrame(records)
        self.multiverse_bridge_lf = result.lazy()
        LOGGER.info(
            f"  multiverse_bridge_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _build_signatures_lookup(self) -> None:
        """
        Build signatures lookup (by setCode + numberPrefix).
        """
        if self.resource_path is None:
            LOGGER.info("  signatures: No resource_path set")
            return

        signatures_path = self.resource_path / "world_championship_signatures.json"
        if not signatures_path.exists():
            LOGGER.info("  signatures: No signatures file found")
            return

        with signatures_path.open(encoding="utf-8") as f:
            signatures_by_set = json.load(f)

        records = []
        for set_code, prefix_map in signatures_by_set.items():
            for prefix, sig_name in prefix_map.items():
                records.append(
                    {
                        "setCode": set_code,
                        "numberPrefix": prefix,
                        "signature": sig_name,
                    }
                )

        if not records:
            LOGGER.info("  signatures: No signature data found")
            return

        result = pl.DataFrame(records)
        self.signatures_lf = result.lazy()
        LOGGER.info(
            f"  signatures_lf: {result.height:,} rows x {len(result.columns)} cols"
        )
