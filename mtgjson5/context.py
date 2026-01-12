"""
Pipeline context for MTGJSON ELT Pipeline.
"""

from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import polars_hash as plh


if TYPE_CHECKING:
    from mtgjson5.cache import GlobalCache
    from mtgjson5.categoricals import DynamicCategoricals

from mtgjson5.constants import LANGUAGE_MAP
from mtgjson5.mtgjson_models import (
    CardAtomic,
    CardDeck,
    CardSet,
    CardToken,
)
from mtgjson5.utils import LOGGER, get_expanded_set_codes


_DNS_NAMESPACE = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"


@dataclass
class PipelineContext:
    """
    Container for pipeline configuration and derived lookups.
    it holds a reference to GlobalCache for raw data access and owns:

        - Pipeline-specific configuration (args, filters)
        - Derived/consolidated lookups (built by consolidate_lookups())
        - Model types and categoricals

    Raw data is accessed through property accessors that delegate to
    the underlying GlobalCache.
    """
    _cache: GlobalCache | None = field(default=None, repr=False)

    args: Namespace | None = None
    scryfall_id_filter: set[str] | None = None

    identifiers_lf: pl.LazyFrame | None = None
    oracle_data_lf: pl.LazyFrame | None = None
    set_number_lf: pl.LazyFrame | None = None
    name_lf: pl.LazyFrame | None = None
    signatures_lf: pl.LazyFrame | None = None
    face_flavor_names_df: pl.DataFrame | None = None
    face_foreign_lf: pl.LazyFrame | None = None
    uuid_lookup_df: pl.DataFrame | None = None
    final_cards_lf: pl.LazyFrame | None = None

    card_set_model: type = field(default=CardSet)
    card_token_model: type = field(default=CardToken)
    card_deck_model: type = field(default=CardDeck)
    card_atomic_model: type = field(default=CardAtomic)

    categoricals: DynamicCategoricals | None = None
    
    resource_path: Path | None = None

    _test_data: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def cards_lf(self) -> pl.LazyFrame | None:
        """Raw card data from cache."""
        if "_cards_lf" in self._test_data:
            return self._test_data["_cards_lf"]
        return self._cache.cards_lf if self._cache else None

    @property
    def raw_rulings_lf(self) -> pl.LazyFrame | None:
        """Raw rulings data from cache."""
        if "_raw_rulings_lf" in self._test_data:
            return self._test_data["_raw_rulings_lf"]
        return self._cache.raw_rulings_lf if self._cache else None

    @property
    def sets_lf(self) -> pl.LazyFrame | None:
        """Set metadata from cache."""
        if "_sets_lf" in self._test_data:
            return self._test_data["_sets_lf"]
        return self._cache.sets_lf if self._cache else None

    @property
    def card_kingdom_lf(self) -> pl.LazyFrame | None:
        """Card Kingdom lookup from cache."""
        if "_card_kingdom_lf" in self._test_data:
            return self._test_data["_card_kingdom_lf"]
        return self._cache.card_kingdom_lf if self._cache else None

    @property
    def card_kingdom_raw_lf(self) -> pl.LazyFrame | None:
        """Card Kingdom raw data from cache."""
        if "_card_kingdom_raw_lf" in self._test_data:
            return self._test_data["_card_kingdom_raw_lf"]
        return self._cache.card_kingdom_raw_lf if self._cache else None

    @property
    def mcm_lookup_lf(self) -> pl.LazyFrame | None:
        """MCM lookup from cache."""
        if "_mcm_lookup_lf" in self._test_data:
            return self._test_data["_mcm_lookup_lf"]
        return self._cache.mcm_lookup_lf if self._cache else None

    @property
    def salt_lf(self) -> pl.LazyFrame | None:
        """EDHREC salt data from cache."""
        if "_salt_lf" in self._test_data:
            return self._test_data["_salt_lf"]
        return self._cache.salt_lf if self._cache else None

    @property
    def spellbook_lf(self) -> pl.LazyFrame | None:
        """Spellbook data from cache."""
        if "_spellbook_lf" in self._test_data:
            return self._test_data["_spellbook_lf"]
        return self._cache.spellbook_lf if self._cache else None

    @property
    def sld_subsets_lf(self) -> pl.LazyFrame | None:
        """Secret Lair subsets from cache."""
        if "_sld_subsets_lf" in self._test_data:
            return self._test_data["_sld_subsets_lf"]
        return self._cache.sld_subsets_lf if self._cache else None

    @property
    def uuid_cache_lf(self) -> pl.LazyFrame | None:
        """UUID cache from cache."""
        if "_uuid_cache_lf" in self._test_data:
            return self._test_data["_uuid_cache_lf"]
        return self._cache.uuid_cache_lf if self._cache else None

    @property
    def orientation_lf(self) -> pl.LazyFrame | None:
        """Orientation data from cache."""
        if "_orientation_lf" in self._test_data:
            return self._test_data["_orientation_lf"]
        return self._cache.orientation_lf if self._cache else None

    @property
    def gatherer_lf(self) -> pl.LazyFrame | None:
        """Gatherer data from cache."""
        if "_gatherer_lf" in self._test_data:
            return self._test_data["_gatherer_lf"]
        return self._cache.gatherer_lf if self._cache else None

    @property
    def rulings_lf(self) -> pl.LazyFrame | None:
        """Rulings from cache."""
        if "_rulings_lf" in self._test_data:
            return self._test_data["_rulings_lf"]
        return self._cache.rulings_lf if self._cache else None

    @property
    def foreign_data_lf(self) -> pl.LazyFrame | None:
        """Foreign data from cache."""
        if "_foreign_data_lf" in self._test_data:
            return self._test_data["_foreign_data_lf"]
        return self._cache.foreign_data_lf if self._cache else None

    @property
    def sealed_cards_lf(self) -> pl.LazyFrame | None:
        """Sealed cards from cache."""
        if "_sealed_cards_lf" in self._test_data:
            return self._test_data["_sealed_cards_lf"]
        return self._cache.sealed_cards_lf if self._cache else None

    @property
    def sealed_products_lf(self) -> pl.LazyFrame | None:
        """Sealed products from cache."""
        if "_sealed_products_lf" in self._test_data:
            return self._test_data["_sealed_products_lf"]
        return self._cache.sealed_products_lf if self._cache else None

    @property
    def sealed_contents_lf(self) -> pl.LazyFrame | None:
        """Sealed contents from cache."""
        if "_sealed_contents_lf" in self._test_data:
            return self._test_data["_sealed_contents_lf"]
        return self._cache.sealed_contents_lf if self._cache else None

    @property
    def decks_lf(self) -> pl.LazyFrame | None:
        """Decks from cache."""
        if "_decks_lf" in self._test_data:
            return self._test_data["_decks_lf"]
        return self._cache.decks_lf if self._cache else None

    @property
    def boosters_lf(self) -> pl.LazyFrame | None:
        """Boosters from cache."""
        if "_boosters_lf" in self._test_data:
            return self._test_data["_boosters_lf"]
        return self._cache.boosters_lf if self._cache else None

    @property
    def card_to_products_lf(self) -> pl.LazyFrame | None:
        """Card to products mapping (alias for sealed_cards_lf)."""
        return self.sealed_cards_lf

    @property
    def tcg_skus_lf(self) -> pl.LazyFrame | None:
        """TCG SKUs from cache."""
        if "_tcg_skus_lf" in self._test_data:
            return self._test_data["_tcg_skus_lf"]
        return self._cache.tcg_skus_lf if self._cache else None

    @property
    def tcg_sku_map_lf(self) -> pl.LazyFrame | None:
        """TCG SKU map from cache."""
        if "_tcg_sku_map_lf" in self._test_data:
            return self._test_data["_tcg_sku_map_lf"]
        return self._cache.tcg_sku_map_lf if self._cache else None

    @property
    def tcg_to_uuid_lf(self) -> pl.LazyFrame | None:
        """TCG to UUID mapping from cache."""
        if "_tcg_to_uuid_lf" in self._test_data:
            return self._test_data["_tcg_to_uuid_lf"]
        return self._cache.tcg_to_uuid_lf if self._cache else None

    @property
    def tcg_etched_to_uuid_lf(self) -> pl.LazyFrame | None:
        """TCG etched to UUID mapping from cache."""
        if "_tcg_etched_to_uuid_lf" in self._test_data:
            return self._test_data["_tcg_etched_to_uuid_lf"]
        return self._cache.tcg_etched_to_uuid_lf if self._cache else None

    @property
    def mtgo_to_uuid_lf(self) -> pl.LazyFrame | None:
        """MTGO to UUID mapping from cache."""
        if "_mtgo_to_uuid_lf" in self._test_data:
            return self._test_data["_mtgo_to_uuid_lf"]
        return self._cache.mtgo_to_uuid_lf if self._cache else None

    @property
    def cardmarket_to_uuid_lf(self) -> pl.LazyFrame | None:
        """Cardmarket to UUID mapping from cache."""
        if "_cardmarket_to_uuid_lf" in self._test_data:
            return self._test_data["_cardmarket_to_uuid_lf"]
        return self._cache.cardmarket_to_uuid_lf if self._cache else None

    @property
    def uuid_to_oracle_lf(self) -> pl.LazyFrame | None:
        """UUID to oracle mapping from cache."""
        if "_uuid_to_oracle_lf" in self._test_data:
            return self._test_data["_uuid_to_oracle_lf"]
        return self._cache.uuid_to_oracle_lf if self._cache else None

    @property
    def default_card_languages_lf(self) -> pl.LazyFrame | None:
        """Default card languages from cache."""
        if "_default_card_languages_lf" in self._test_data:
            return self._test_data["_default_card_languages_lf"]
        return self._cache.default_card_languages_lf if self._cache else None

    @property
    def meld_triplets(self) -> dict[str, list[str]]:
        """Meld triplets from cache."""
        if "_meld_triplets" in self._test_data:
            return self._test_data["_meld_triplets"]
        return self._cache.meld_triplets if self._cache else {}

    @property
    def manual_overrides(self) -> dict:
        """Manual overrides from cache."""
        if "_manual_overrides" in self._test_data:
            return self._test_data["_manual_overrides"]
        return self._cache.manual_overrides if self._cache else {}

    @property
    def foreigndata_exceptions(self) -> dict:
        """Foreign data exceptions from cache."""
        if "_foreigndata_exceptions" in self._test_data:
            return self._test_data["_foreigndata_exceptions"]
        return self._cache.foreigndata_exceptions if self._cache else {}

    @property
    def gatherer_map(self) -> dict:
        """Gatherer map from cache."""
        if "_gatherer_map" in self._test_data:
            return self._test_data["_gatherer_map"]
        return self._cache.gatherer_map if self._cache else {}

    @property
    def standard_legal_sets(self) -> set[str]:
        """Standard legal sets from cache."""
        if "_standard_legal_sets" in self._test_data:
            return self._test_data["_standard_legal_sets"]
        return self._cache.standard_legal_sets if self._cache else set()

    @property
    def unlimited_cards(self) -> set[str]:
        """Unlimited cards from cache."""
        if "_unlimited_cards" in self._test_data:
            return self._test_data["_unlimited_cards"]
        return self._cache.unlimited_cards if self._cache else set()

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
        Create a PipelineContext backed by the global cache.

        This is the primary factory method for production use.
        """
        from mtgjson5 import constants
        from mtgjson5.cache import GLOBAL_CACHE
        from mtgjson5.categoricals import discover_categoricals

        # Discover categoricals from the raw cards data
        categoricals = None
        if GLOBAL_CACHE.cards_lf is not None:
            categoricals = discover_categoricals(
                GLOBAL_CACHE.cards_lf,
                GLOBAL_CACHE.sets_lf,
            )

        return cls(
            _cache=GLOBAL_CACHE,
            args=args,
            categoricals=categoricals,
            scryfall_id_filter=GLOBAL_CACHE.scryfall_id_filter,
            resource_path=constants.RESOURCE_PATH,
        )

    @classmethod
    def for_testing(
        cls,
        cards_lf: pl.LazyFrame | None = None,
        sets_lf: pl.LazyFrame | None = None,
        raw_rulings_lf: pl.LazyFrame | None = None,
        uuid_cache_lf: pl.LazyFrame | None = None,
        card_kingdom_lf: pl.LazyFrame | None = None,
        salt_lf: pl.LazyFrame | None = None,
        orientation_lf: pl.LazyFrame | None = None,
        meld_triplets: dict[str, list[str]] | None = None,
        manual_overrides: dict | None = None,
        foreigndata_exceptions: dict | None = None,
        resource_path: Path | None = None,
        args: Namespace | None = None,
        **kwargs: Any,
    ) -> PipelineContext:
        """
        Create a PipelineContext with explicit test data.

        This factory allows testing pipeline functions without
        requiring GlobalCache to be loaded.
        """
        test_data: dict[str, Any] = {}

        if cards_lf is not None:
            test_data["_cards_lf"] = cards_lf
        if sets_lf is not None:
            test_data["_sets_lf"] = sets_lf
        if raw_rulings_lf is not None:
            test_data["_raw_rulings_lf"] = raw_rulings_lf
        if uuid_cache_lf is not None:
            test_data["_uuid_cache_lf"] = uuid_cache_lf
        if card_kingdom_lf is not None:
            test_data["_card_kingdom_lf"] = card_kingdom_lf
        if salt_lf is not None:
            test_data["_salt_lf"] = salt_lf
        if orientation_lf is not None:
            test_data["_orientation_lf"] = orientation_lf
        if meld_triplets is not None:
            test_data["_meld_triplets"] = meld_triplets
        if manual_overrides is not None:
            test_data["_manual_overrides"] = manual_overrides
        if foreigndata_exceptions is not None:
            test_data["_foreigndata_exceptions"] = foreigndata_exceptions

        for key, value in kwargs.items():
            if key.startswith("_") or not key.endswith("_lf"):
                test_data[f"_{key}"] = value
            else:
                test_data[key] = value

        return cls(
            _cache=None,
            _test_data=test_data,
            args=args,
            resource_path=resource_path,
        )

    def consolidate_lookups(self) -> PipelineContext:
        """
        Consolidate separate lookup tables into combined tables by join key.

        This is where PipelineContext does actual work - building derived
        lookups from the raw cached data.
        """
        LOGGER.info("Consolidating lookup tables...")

        self._build_identifiers_lookup()
        self._build_oracle_data_lookup()
        self._build_set_number_lookup()
        self._build_name_lookup()
        self._build_signatures_lookup()
        self._load_face_flavor_names()

        return self

    def _build_identifiers_lookup(self) -> None:
        """
        Build consolidated identifiers lookup (by scryfallId + side).
        """
        uuid_cache_raw = self.uuid_cache_lf
        if uuid_cache_raw is None:
            LOGGER.info("identifiers: No uuid_cache_lf, skipping")
            return

        if isinstance(uuid_cache_raw, pl.LazyFrame):
            uuid_cache: pl.DataFrame = uuid_cache_raw.collect()
        else:
            uuid_cache = uuid_cache_raw

        if uuid_cache.height == 0:
            LOGGER.info("identifiers: uuid_cache_lf is empty, skipping")
            return

        result: pl.DataFrame = uuid_cache.select(["scryfallId", "side", "cachedUuid"])
        LOGGER.info(f"identifiers: +uuid_cache ({result.height:,} rows)")

        # Add Card Kingdom data (by scryfallId only, duplicated for all sides)
        ck_raw = self.card_kingdom_lf
        if ck_raw is not None:
            if isinstance(ck_raw, pl.LazyFrame):
                ck: pl.DataFrame = ck_raw.collect()
            else:
                ck = ck_raw
            if ck.height > 0:
                ck = ck.rename({"id": "scryfallId"}).select(
                    [
                        "scryfallId",
                        "cardKingdomId",
                        "cardKingdomFoilId",
                        "cardKingdomEtchedId",
                        "cardKingdomUrl",
                        "cardKingdomFoilUrl",
                        "cardKingdomEtchedUrl",
                    ]
                )
                result = result.join(ck, on="scryfallId", how="left")
                LOGGER.info(f"identifiers: +card_kingdom ({ck.height:,} rows)")

        # Add orientation data (by scryfallId only)
        orient_raw = self.orientation_lf
        if orient_raw is not None:
            if isinstance(orient_raw, pl.LazyFrame):
                orient: pl.DataFrame = orient_raw.collect()
            else:
                orient = orient_raw
            if orient.height > 0:
                result = result.join(orient, on="scryfallId", how="left")
                LOGGER.info(f"identifiers: +orientation ({orient.height:,} rows)")

        self.identifiers_lf = result.lazy()
        LOGGER.info(
            f"identifiers_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _build_oracle_data_lookup(self) -> None:
        """
        Build consolidated oracle data lookup (by oracleId).
        """
        frames: list[tuple[str, pl.DataFrame]] = []

        # Salt data
        salt_raw = self.salt_lf
        if salt_raw is not None:
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
                LOGGER.info(f"oracle_data: +salt ({salt.height:,} rows)")

        # Rulings (aggregate by oracleId into list of structs)
        rulings_raw = self.raw_rulings_lf
        if rulings_raw is not None:
            if isinstance(rulings_raw, pl.LazyFrame):
                rulings: pl.DataFrame = rulings_raw.collect()
            else:
                rulings = rulings_raw
            if rulings.height > 0:
                rulings_agg = (
                    rulings.sort("publishedAt", descending=True)
                    .group_by("oracleId")
                    .agg(
                        pl.struct(["source", "publishedAt", "comment"]).alias("rulings")
                    )
                )
                frames.append(("rulings", rulings_agg))
                LOGGER.info(f"oracle_data: +rulings ({rulings_agg.height:,} rows)")

        # Printings + originalReleaseDate (computed from cards_lf)
        cards_raw = self.cards_lf
        if cards_raw is not None:
            if isinstance(cards_raw, pl.LazyFrame):
                cards: pl.DataFrame = cards_raw.collect()
            else:
                cards = cards_raw

            # For multi-face cards, oracle_id may be null at card level but present in card_faces
            # Coalesce from first face's oracle_id if available
            printings = (
                cards.with_columns(
                    pl.coalesce(
                        pl.col("oracleId"),
                        pl.col("cardFaces").list.get(0).struct.field("oracle_id"),
                    ).alias("_effectiveOracleId")
                )
                .select(["_effectiveOracleId", "set"])
                .filter(pl.col("_effectiveOracleId").is_not_null())
                .group_by("_effectiveOracleId")
                .agg(
                    pl.col("set").str.to_uppercase().unique().sort().alias("printings")
                )
                .rename({"_effectiveOracleId": "oracleId"})
            )

            if printings.height > 0:
                frames.append(("printings", printings))
                LOGGER.info(
                    f"oracle_data: +printings ({printings.height:,} rows)"
                )

        if not frames:
            LOGGER.info("oracle_data: No data to consolidate")
            return

        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(df, on="oracleId", how="full", coalesce=True)

        self.oracle_data_lf = result.lazy()
        LOGGER.info(
            f"oracle_data_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _build_set_number_lookup(self) -> None:
        """
        If you're reading this function... don't.
        Just know that it do what it do and it do it good.
        Builds consolidated lookup by setCode + number...
        Also aggregates foreign data...
        Also applies foreign data exceptions...
        Also generated UUIDs for foreign cards...
        Also duel-deck stuff...
        I'm sorry.
        """
        frames: list[tuple[str, pl.DataFrame]] = []

        # Foreign data: aggregate non-English cards by set+number
        cards_raw = self.cards_lf
        if cards_raw is not None:
            if isinstance(cards_raw, pl.LazyFrame):
                cards: pl.DataFrame = cards_raw.collect()
            else:
                cards = cards_raw

            # Build default language card lookup for UUID generation
            default_card_languages_raw = self.default_card_languages_lf
            if default_card_languages_raw is not None:
                default_lang_df = default_card_languages_raw
                if isinstance(default_lang_df, pl.LazyFrame):
                    default_lang_df = default_lang_df.collect()

                default_lang_lookup = (
                    cards.with_columns([
                        pl.col("set").str.to_uppercase().alias("setCode"),
                        pl.col("lang")
                        .replace_strict(LANGUAGE_MAP, default=pl.col("lang"))
                        .alias("_lang_full"),
                    ])
                    .join(
                        default_lang_df,
                        left_on=["id", "_lang_full"],
                        right_on=["scryfallId", "language"],
                        how="semi",
                    )
                    .select([
                        "setCode",
                        pl.col("collectorNumber").alias("number"),
                        pl.col("id").alias("_default_scryfall_id"),
                        pl.lit("a").alias("_default_side"),
                    ])
                )
            else:
                default_lang_lookup = (
                    cards.filter(pl.col("lang") == "en")
                    .with_columns(pl.col("set").str.to_uppercase().alias("setCode"))
                    .select([
                        "setCode",
                        pl.col("collectorNumber").alias("number"),
                        pl.col("id").alias("_default_scryfall_id"),
                        pl.lit("a").alias("_default_side"),
                    ])
                )

            # Parse foreignData exceptions
            fd_include: set[tuple[str, str]] = set()
            fd_exclude: set[tuple[str, str]] = set()
            foreigndata_exceptions = self.foreigndata_exceptions
            if foreigndata_exceptions:
                for set_code, numbers in foreigndata_exceptions.get("include", {}).items():
                    if not set_code.startswith("_"):
                        for number in numbers:
                            if not number.startswith("_"):
                                fd_include.add((set_code, number))
                for set_code, numbers in foreigndata_exceptions.get("exclude", {}).items():
                    if not set_code.startswith("_"):
                        for number in numbers:
                            if not number.startswith("_"):
                                fd_exclude.add((set_code, number))

            foreign_df = (
                cards.filter(pl.col("lang") != "en")
                .with_columns(pl.col("set").str.to_uppercase().alias("setCode"))
                .filter(
                    ~pl.col("collectorNumber").str.contains(r"[sd†]$")
                    | pl.struct(["setCode", "collectorNumber"]).is_in(
                        [{"setCode": s, "collectorNumber": n} for s, n in fd_include]
                    )
                )
                .filter(
                    ~pl.struct(["setCode", "collectorNumber"]).is_in(
                        [{"setCode": s, "collectorNumber": n} for s, n in fd_exclude]
                    )
                    if fd_exclude
                    else pl.lit(True)
                )
                .join(
                    default_lang_lookup,
                    left_on=["setCode", "collectorNumber"],
                    right_on=["setCode", "number"],
                    how="left",
                )
                .with_columns(
                    [
                        pl.col("lang")
                        .replace_strict(
                            LANGUAGE_MAP,
                            default=pl.col("lang"),
                        )
                        .alias("language"),
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
                        .otherwise(pl.col("printedName"))
                        .alias("_foreign_name"),
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
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.col("cardFaces")
                            .list.first()
                            .struct.field("flavor_text")
                        )
                        .otherwise(pl.col("flavorText"))
                        .alias("_flavor_text"),
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.col("cardFaces")
                            .list.first()
                            .struct.field("printed_text")
                        )
                        .otherwise(pl.col("printedText"))
                        .alias("_foreign_text"),
                        pl.when(pl.col("cardFaces").list.len() > 1)
                        .then(
                            pl.col("cardFaces")
                            .list.first()
                            .struct.field("printed_type_line")
                        )
                        .otherwise(pl.col("printedTypeLine"))
                        .alias("_foreign_type"),
                        pl.concat_str(
                            [
                                pl.col("_default_scryfall_id"),
                                pl.col("_default_side"),
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
                .with_columns(
                    pl.col("_uuid_source"),
                    plh.col("_uuid_source").uuidhash.uuid5(_DNS_NAMESPACE)
                    .alias("_foreign_uuid")
                )
                .filter(pl.col("_foreign_name").is_not_null())
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
                            pl.col("multiverseIds")
                            .list.first()
                            .alias("multiverseId"),
                            pl.col("_foreign_name").alias("name"),
                            pl.col("_foreign_text").alias("text"),
                            pl.col("_foreign_type").alias("type"),
                            pl.col("_foreign_uuid").alias("uuid"),
                        ]
                    ).alias("foreignData")
                )
            )

            if foreign_df.height > 0:
                frames.append(("foreign", foreign_df))
                LOGGER.info(f"set_number: +foreign ({foreign_df.height:,} rows)")

        # Duel deck sides from JSON
        duel_deck_df = self._load_duel_deck_sides()
        if duel_deck_df is not None and duel_deck_df.height > 0:
            frames.append(("duel_deck", duel_deck_df))
            LOGGER.info(f"set_number: +duel_deck ({duel_deck_df.height:,} rows)")

        if not frames:
            LOGGER.info("set_number: No data to consolidate")
            return

        result: pl.DataFrame = frames[0][1]
        for _, df in frames[1:]:
            result = result.join(
                df, on=["setCode", "number"], how="full", coalesce=True
            )

        self.set_number_lf = result.lazy()
        LOGGER.info(
            f"set_number_lf: {result.height:,} rows x {len(result.columns)} cols"
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
        spellbook_raw = self.spellbook_lf
        if spellbook_raw is not None:
            if isinstance(spellbook_raw, pl.LazyFrame):
                spellbook: pl.DataFrame = spellbook_raw.collect()
            else:
                spellbook = spellbook_raw
            if spellbook.height > 0:
                frames.append(("spellbook", spellbook))
                LOGGER.info(f"name: +spellbook ({spellbook.height:,} rows)")

        # Meld triplets (convert dict to DataFrame)
        meld_triplets = self.meld_triplets
        if meld_triplets:
            meld_records = [
                {"name": name, "cardParts": parts}
                for name, parts in meld_triplets.items()
            ]
            meld_df = pl.DataFrame(meld_records)
            frames.append(("meld", meld_df))
            LOGGER.info(f"name: +meld ({meld_df.height:,} rows)")

        if not frames:
            LOGGER.info("name: No data to consolidate")
            return

        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(df, on="name", how="full", coalesce=True)

        self.name_lf = result.lazy()
        LOGGER.info(f"name_lf: {result.height:,} rows x {len(result.columns)} cols")

    def _build_signatures_lookup(self) -> None:
        """
        Build signatures lookup (by setCode + numberPrefix).
        """
        if self.resource_path is None:
            LOGGER.info("signatures: No resource_path set")
            return

        signatures_path = self.resource_path / "world_championship_signatures.json"
        if not signatures_path.exists():
            LOGGER.info("signatures: No signatures file found")
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
            LOGGER.info("signatures: No signature data found")
            return

        result = pl.DataFrame(records)
        self.signatures_lf = result.lazy()
        LOGGER.info(
            f"signatures_lf: {result.height:,} rows x {len(result.columns)} cols"
        )

    def _load_face_flavor_names(self) -> None:
        """
        Load face flavor names from resource JSON.

        Converts the JSON format (scryfallId_side keys) to a DataFrame
        with columns: scryfallId, side, _faceFlavorName, _flavorNameOverride
        """
        if self.resource_path is None:
            LOGGER.info("face_flavor_names: No resource_path set")
            return

        flavor_path = self.resource_path / "face_flavor_names.json"
        if not flavor_path.exists():
            LOGGER.info("face_flavor_names: No file found")
            return

        with flavor_path.open(encoding="utf-8") as f:
            flavor_data = json.load(f)

        records = []
        for key, data in flavor_data.items():
            if "_" not in key:
                continue
            scryfall_id, side = key.rsplit("_", 1)
            records.append({
                "scryfallId": scryfall_id,
                "side": side,
                "_faceFlavorName": data.get("faceFlavorName"),
                "_flavorNameOverride": data.get("flavorName"),
            })

        if not records:
            LOGGER.info("face_flavor_names: No data found")
            return

        self.face_flavor_names_df = pl.DataFrame(records)
        LOGGER.info(
            f"face_flavor_names_df: {self.face_flavor_names_df.height:,} rows"
        )
