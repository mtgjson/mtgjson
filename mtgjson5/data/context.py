"""
Pipeline context for MTGJSON ELT Pipeline.
"""

from __future__ import annotations

import json
from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl
import polars_hash as plh

if TYPE_CHECKING:
    from mtgjson5.data.cache import GlobalCache
    from mtgjson5.polars_utils import DynamicCategoricals

from mtgjson5.consts import LANGUAGE_MAP
from mtgjson5.models import (
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
    watermark_overrides_lf: pl.LazyFrame | None = None
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

    mcm_set_map: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    _mcm_lookup_enriched: pl.LazyFrame | None = field(default=None, repr=False)

    @property
    def cards_lf(self) -> pl.LazyFrame | None:
        """Raw card data from cache."""
        if "_cards_lf" in self._test_data:
            return cast("pl.LazyFrame | None", self._test_data["_cards_lf"])
        return self._cache.cards_lf if self._cache else None

    @property
    def rulings_lf(self) -> pl.LazyFrame | None:
        """Raw rulings data from cache."""
        if "_rulings_lf" in self._test_data:
            return self._test_data["_rulings_lf"]  # type: ignore[no-any-return]
        return self._cache.rulings_lf if self._cache else None

    @property
    def sets_lf(self) -> pl.LazyFrame | None:
        """Set metadata from cache."""
        if "_sets_lf" in self._test_data:
            return self._test_data["_sets_lf"]  # type: ignore[no-any-return]
        return self._cache.sets_lf if self._cache else None

    @property
    def card_kingdom_lf(self) -> pl.LazyFrame | None:
        """Card Kingdom lookup from cache."""
        if "_card_kingdom_lf" in self._test_data:
            return self._test_data["_card_kingdom_lf"]  # type: ignore[no-any-return]
        return self._cache.card_kingdom_lf if self._cache else None

    @property
    def card_kingdom_raw_lf(self) -> pl.LazyFrame | None:
        """Card Kingdom raw data from cache."""
        if "_card_kingdom_raw_lf" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_card_kingdom_raw_lf"
            ]
        return self._cache.card_kingdom_raw_lf if self._cache else None

    @property
    def mcm_lookup_lf(self) -> pl.LazyFrame | None:
        """MCM lookup from cache (enriched with setCode after consolidate_lookups)."""
        if self._mcm_lookup_enriched is not None:
            return self._mcm_lookup_enriched
        if "_mcm_lookup_lf" in self._test_data:
            return self._test_data["_mcm_lookup_lf"]  # type: ignore[no-any-return]
        return self._cache.mcm_lookup_lf if self._cache else None

    @property
    def salt_lf(self) -> pl.LazyFrame | None:
        """EDHREC salt data from cache."""
        if "_salt_lf" in self._test_data:
            return self._test_data["_salt_lf"]  # type: ignore[no-any-return]
        return self._cache.salt_lf if self._cache else None

    @property
    def spellbook_lf(self) -> pl.LazyFrame | None:
        """Spellbook data from cache."""
        if "_spellbook_lf" in self._test_data:
            return self._test_data["_spellbook_lf"]  # type: ignore[no-any-return]
        return self._cache.spellbook_lf if self._cache else None

    @property
    def sld_subsets_lf(self) -> pl.LazyFrame | None:
        """Secret Lair subsets from cache."""
        if "_sld_subsets_lf" in self._test_data:
            return self._test_data["_sld_subsets_lf"]  # type: ignore[no-any-return]
        return self._cache.sld_subsets_lf if self._cache else None

    @property
    def uuid_cache_lf(self) -> pl.LazyFrame | None:
        """UUID cache from cache."""
        if "_uuid_cache_lf" in self._test_data:
            return self._test_data["_uuid_cache_lf"]  # type: ignore[no-any-return]
        return self._cache.uuid_cache_lf if self._cache else None

    @property
    def orientation_lf(self) -> pl.LazyFrame | None:
        """Orientation data from cache."""
        if "_orientation_lf" in self._test_data:
            return self._test_data["_orientation_lf"]  # type: ignore[no-any-return]
        return self._cache.orientation_lf if self._cache else None

    @property
    def gatherer_lf(self) -> pl.LazyFrame | None:
        """Gatherer data from cache."""
        if "_gatherer_lf" in self._test_data:
            return self._test_data["_gatherer_lf"]  # type: ignore[no-any-return]
        return self._cache.gatherer_lf if self._cache else None

    @property
    def multiverse_bridge_lf(self) -> pl.LazyFrame | None:
        """Multiverse bridge data (cardsphere, deckbox IDs) from cache."""
        if "_multiverse_bridge_lf" in self._test_data:
            return self._test_data["_multiverse_bridge_lf"]  # type: ignore[no-any-return]
        return self._cache.multiverse_bridge_lf if self._cache else None

    @property
    def sealed_cards_lf(self) -> pl.LazyFrame | None:
        """Sealed cards from cache."""
        if "_sealed_cards_lf" in self._test_data:
            return self._test_data["_sealed_cards_lf"]  # type: ignore[no-any-return]
        return self._cache.sealed_cards_lf if self._cache else None

    @property
    def sealed_products_lf(self) -> pl.LazyFrame | None:
        """Sealed products from cache."""
        if "_sealed_products_lf" in self._test_data:
            return self._test_data["_sealed_products_lf"]  # type: ignore[no-any-return]
        return self._cache.sealed_products_lf if self._cache else None

    @property
    def sealed_contents_lf(self) -> pl.LazyFrame | None:
        """Sealed contents from cache."""
        if "_sealed_contents_lf" in self._test_data:
            return self._test_data["_sealed_contents_lf"]  # type: ignore[no-any-return]
        return self._cache.sealed_contents_lf if self._cache else None

    @property
    def decks_lf(self) -> pl.LazyFrame | None:
        """Decks from cache."""
        if "_decks_lf" in self._test_data:
            return self._test_data["_decks_lf"]  # type: ignore[no-any-return]
        return self._cache.decks_lf if self._cache else None

    @property
    def boosters_lf(self) -> pl.LazyFrame | None:
        """Boosters from cache."""
        if "_boosters_lf" in self._test_data:
            return self._test_data["_boosters_lf"]  # type: ignore[no-any-return]
        return self._cache.boosters_lf if self._cache else None

    @property
    def token_products_lf(self) -> pl.LazyFrame | None:
        """Token products mapping from cache."""
        if "_token_products_lf" in self._test_data:
            return self._test_data["_token_products_lf"]  # type: ignore[no-any-return]
        return self._cache.token_products_lf if self._cache else None

    @property
    def card_to_products_lf(self) -> pl.LazyFrame | None:
        """Card to products mapping (alias for sealed_cards_lf)."""
        return self.sealed_cards_lf

    @property
    def tcg_skus_lf(self) -> pl.LazyFrame | None:
        """TCG SKUs from cache."""
        if "_tcg_skus_lf" in self._test_data:
            return self._test_data["_tcg_skus_lf"]  # type: ignore[no-any-return]
        return self._cache.tcg_skus_lf if self._cache else None

    @property
    def tcg_sku_map_lf(self) -> pl.LazyFrame | None:
        """TCG SKU map from cache."""
        if "_tcg_sku_map_lf" in self._test_data:
            return self._test_data["_tcg_sku_map_lf"]  # type: ignore[no-any-return]
        return self._cache.tcg_sku_map_lf if self._cache else None

    @property
    def tcg_to_uuid_lf(self) -> pl.LazyFrame | None:
        """TCG to UUID mapping from cache."""
        if "_tcg_to_uuid_lf" in self._test_data:
            return self._test_data["_tcg_to_uuid_lf"]  # type: ignore[no-any-return]
        return self._cache.tcg_to_uuid_lf if self._cache else None

    @property
    def tcg_etched_to_uuid_lf(self) -> pl.LazyFrame | None:
        """TCG etched to UUID mapping from cache."""
        if "_tcg_etched_to_uuid_lf" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_tcg_etched_to_uuid_lf"
            ]
        return self._cache.tcg_etched_to_uuid_lf if self._cache else None

    @property
    def mtgo_to_uuid_lf(self) -> pl.LazyFrame | None:
        """MTGO to UUID mapping from cache."""
        if "_mtgo_to_uuid_lf" in self._test_data:
            return self._test_data["_mtgo_to_uuid_lf"]  # type: ignore[no-any-return]
        return self._cache.mtgo_to_uuid_lf if self._cache else None

    @property
    def cardmarket_to_uuid_lf(self) -> pl.LazyFrame | None:
        """Cardmarket to UUID mapping from cache."""
        if "_cardmarket_to_uuid_lf" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_cardmarket_to_uuid_lf"
            ]
        return self._cache.cardmarket_to_uuid_lf if self._cache else None

    @property
    def languages_lf(self) -> pl.LazyFrame | None:
        """Default card languages from cache."""
        if "_languages_lf" in self._test_data:
            return self._test_data["_languages_lf"]  # type: ignore[no-any-return]
        return self._cache.languages_lf if self._cache else None

    @property
    def meld_triplets(self) -> dict[str, list[str]]:
        """Meld triplets from cache."""
        if "_meld_triplets" in self._test_data:
            return self._test_data["_meld_triplets"]  # type: ignore[no-any-return]
        return self._cache.meld_triplets if self._cache else {}

    @property
    def meld_overrides(self) -> dict:
        """Meld overrides (uuid -> otherFaceIds, cardParts) from cache."""
        if "_meld_overrides" in self._test_data:
            return self._test_data["_meld_overrides"]  # type: ignore[no-any-return]
        return self._cache.meld_overrides if self._cache else {}

    @property
    def manual_overrides(self) -> dict:
        """Manual overrides from cache."""
        if "_manual_overrides" in self._test_data:
            return self._test_data["_manual_overrides"]  # type: ignore[no-any-return]
        return self._cache.manual_overrides if self._cache else {}

    @property
    def foreigndata_exceptions(self) -> dict:
        """Foreign data exceptions from cache."""
        if "_foreigndata_exceptions" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_foreigndata_exceptions"
            ]
        return self._cache.foreigndata_exceptions if self._cache else {}

    @property
    def gatherer_map(self) -> dict:
        """Gatherer map from cache."""
        if "_gatherer_map" in self._test_data:
            return self._test_data["_gatherer_map"]  # type: ignore[no-any-return]
        return self._cache.gatherer_map if self._cache else {}

    @property
    def set_code_watermarks(self) -> dict:
        """Set code watermarks from cache."""
        if "_set_code_watermarks" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_set_code_watermarks"
            ]
        return self._cache.set_code_watermarks if self._cache else {}

    @property
    def standard_legal_sets(self) -> set[str]:
        """Standard legal sets from cache."""
        if "_standard_legal_sets" in self._test_data:
            return self._test_data[  # type: ignore[no-any-return]
                "_standard_legal_sets"
            ]
        return self._cache.standard_legal_sets if self._cache else set()

    @property
    def unlimited_cards(self) -> set[str]:
        """Unlimited cards from cache."""
        if "_unlimited_cards" in self._test_data:
            return self._test_data["_unlimited_cards"]  # type: ignore[no-any-return]
        return self._cache.unlimited_cards if self._cache else set()

    @property
    def card_enrichment(self) -> dict[str, dict[str, dict]]:
        """Card enrichment data from cache."""
        if "_card_enrichment" in self._test_data:
            return self._test_data["_card_enrichment"]  # type: ignore[no-any-return]
        return self._cache.card_enrichment if self._cache else {}

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
        if formats_raw and isinstance(formats_raw, (list, tuple, set)):
            return {f.lower() for f in list(formats_raw)}
        return None

    @property
    def sets_to_build(self) -> set[str] | None:
        """Set of set codes to build."""
        if not self.args:
            return None
        arg_sets = getattr(self.args, "sets", None)
        sets = get_expanded_set_codes(arg_sets)
        return {s.upper() for s in sets} if sets else None

    def get_mcm_extras_set_id(self, set_name: str) -> int | None:
        """
        Get MKM 'Extras' set ID (e.g. 'Throne of Eldraine: Extras').

        :param set_name: Scryfall set name
        :return: MCM expansion ID for the extras set, or None
        """
        extras_key = f"{set_name.lower()}: extras"
        entry = self.mcm_set_map.get(extras_key)
        return int(entry["mcmId"]) if entry else None

    @classmethod
    def from_global_cache(cls, args: Namespace | None = None) -> PipelineContext:
        """
        Create a PipelineContext backed by the global cache.

        This is the primary factory method for production use.
        """
        from mtgjson5 import constants
        from mtgjson5.data.cache import GLOBAL_CACHE
        from mtgjson5.polars_utils import discover_categoricals

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
            scryfall_id_filter=GLOBAL_CACHE._scryfall_id_filter,
            resource_path=constants.RESOURCE_PATH,
        )

    @classmethod
    def for_testing(
        cls,
        cards_lf: pl.LazyFrame | None = None,
        sets_lf: pl.LazyFrame | None = None,
        rulings_lf: pl.LazyFrame | None = None,
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
        if rulings_lf is not None:
            test_data["_rulings_lf"] = rulings_lf
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
        self._build_watermark_overrides_lookup()
        self._load_face_flavor_names()
        self._build_mcm_set_map()
        self._build_mcm_lookup()

        return self

    def _build_identifiers_lookup(self) -> None:
        """
        Build consolidated identifiers lookup (by scryfallId + side).

        Uses uuid_cache as primary base, then FULL joins CK data to build
        a superset that includes cards from both sources.
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

        # Add Card Kingdom data - use FULL join to include CK-only cards (new sets)
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
                # Full join: keep all uuid_cache cards AND all CK-only cards
                result = result.join(ck, on="scryfallId", how="full", coalesce=True)
                LOGGER.info(f"identifiers: +card_kingdom ({ck.height:,} rows)")

        # Fill null side values for CK-only rows (cards not in uuid_cache)
        result = result.with_columns(pl.col("side").fill_null("a"))

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

        # Add multiverse bridge data (cardsphere, deckbox IDs) by cachedUuid
        mvb_raw = self.multiverse_bridge_lf
        if mvb_raw is not None:
            if isinstance(mvb_raw, pl.LazyFrame):
                mvb: pl.DataFrame = mvb_raw.collect()
            else:
                mvb = mvb_raw
            if mvb.height > 0 and "cachedUuid" in result.columns:
                result = result.join(mvb, on="cachedUuid", how="left")
                LOGGER.info(f"identifiers: +multiverse_bridge ({mvb.height:,} rows)")

        self.identifiers_lf = result.lazy()
        LOGGER.info(f"identifiers_lf: {result.height:,} rows x {len(result.columns)} cols")

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
        rulings_raw = self.rulings_lf
        if rulings_raw is not None:
            if isinstance(rulings_raw, pl.LazyFrame):
                rulings: pl.DataFrame = rulings_raw.collect()
            else:
                rulings = rulings_raw
            if rulings.height > 0:
                rulings_agg = (
                    rulings.sort("publishedAt", descending=True)
                    .group_by("oracleId")
                    .agg(pl.struct(["source", "publishedAt", "comment"]).alias("rulings"))
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
                .agg(pl.col("set").str.to_uppercase().unique().sort().alias("printings"))
                .rename({"_effectiveOracleId": "oracleId"})
            )

            if printings.height > 0:
                frames.append(("printings", printings))
                LOGGER.info(f"oracle_data: +printings ({printings.height:,} rows)")

        if not frames:
            LOGGER.info("oracle_data: No data to consolidate")
            return

        result: pl.DataFrame = frames[0][1]
        for _name, df in frames[1:]:
            result = result.join(df, on="oracleId", how="full", coalesce=True)

        self.oracle_data_lf = result.lazy()
        LOGGER.info(f"oracle_data_lf: {result.height:,} rows x {len(result.columns)} cols")

    def _build_set_number_lookup(self) -> None:
        """Build consolidated lookup by setCode + number.

        Aggregates foreign data, applies exceptions, generates foreign UUIDs,
        and includes duel-deck side data.
        """
        frames: list[tuple[str, pl.DataFrame]] = []

        # Foreign data: aggregate non-English cards by set+number
        cards_raw = self.cards_lf
        if cards_raw is not None:
            if isinstance(cards_raw, pl.LazyFrame):
                cards: pl.DataFrame = cards_raw.collect()
            else:
                cards = cards_raw

            default_lang_lookup = self._build_default_language_lookup(cards)
            fd_include, fd_exclude = self._parse_foreign_data_exceptions()
            foreign_df = self._build_foreign_data_df(cards, default_lang_lookup, fd_include, fd_exclude)

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
            result = result.join(df, on=["setCode", "number"], how="full", coalesce=True)

        self.set_number_lf = result.lazy()
        LOGGER.info(f"set_number_lf: {result.height:,} rows x {len(result.columns)} cols")

    def _build_default_language_lookup(self, cards: pl.DataFrame) -> pl.DataFrame:
        """Build default language card lookup for foreign UUID generation."""
        languages_raw = self.languages_lf
        if languages_raw is not None:
            default_lang_df = languages_raw
            if isinstance(default_lang_df, pl.LazyFrame):
                default_lang_df = default_lang_df.collect()  # type: ignore[assignment]

            return (
                cards.with_columns(
                    [
                        pl.col("set").str.to_uppercase().alias("setCode"),
                        pl.col("lang").replace_strict(LANGUAGE_MAP, default=pl.col("lang")).alias("_lang_full"),
                    ]
                )
                .join(
                    default_lang_df,  # type: ignore[arg-type]
                    left_on=["id", "_lang_full"],
                    right_on=["scryfallId", "language"],
                    how="semi",
                )
                .select(
                    [
                        "setCode",
                        pl.col("collectorNumber").alias("number"),
                        pl.col("id").alias("_default_scryfall_id"),
                        pl.lit("a").alias("_default_side"),
                    ]
                )
                .unique(subset=["setCode", "number"])
            )

        return (
            cards.filter(pl.col("lang") == "en")
            .with_columns(pl.col("set").str.to_uppercase().alias("setCode"))
            .select(
                [
                    "setCode",
                    pl.col("collectorNumber").alias("number"),
                    pl.col("id").alias("_default_scryfall_id"),
                    pl.lit("a").alias("_default_side"),
                ]
            )
            .unique(subset=["setCode", "number"])
        )

    def _parse_foreign_data_exceptions(self) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Parse foreignData exceptions into include/exclude sets."""
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
        return fd_include, fd_exclude

    def _build_foreign_data_df(
        self,
        cards: pl.DataFrame,
        default_lang_lookup: pl.DataFrame,
        fd_include: set[tuple[str, str]],
        fd_exclude: set[tuple[str, str]],
    ) -> pl.DataFrame:
        """Build aggregated foreign data DataFrame by setCode + number."""
        return (
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
                            pl.col("cardFaces").list.first().struct.field("printed_name"),
                            pl.col("cardFaces").list.first().struct.field("name"),
                        )
                    )
                    .otherwise(None)
                    .alias("_face_name"),
                    pl.when(pl.col("cardFaces").list.len() > 1)
                    .then(pl.col("cardFaces").list.first().struct.field("flavor_text"))
                    .otherwise(pl.col("flavorText"))
                    .alias("_flavor_text"),
                    pl.when(pl.col("cardFaces").list.len() > 1)
                    .then(pl.col("cardFaces").list.first().struct.field("printed_text"))
                    .otherwise(pl.col("printedText"))
                    .alias("_foreign_text"),
                    pl.when(pl.col("cardFaces").list.len() > 1)
                    .then(pl.col("cardFaces").list.first().struct.field("printed_type_line"))
                    .otherwise(pl.col("printedTypeLine"))
                    .alias("_foreign_type"),
                    pl.concat_str(
                        [
                            pl.col("_default_scryfall_id"),
                            pl.col("_default_side"),
                            pl.lit("_"),
                            pl.col("lang").replace_strict(
                                LANGUAGE_MAP,
                                default=pl.col("lang"),
                            ),
                        ]
                    ).alias("_uuid_source"),
                ]
            )
            .with_columns(
                pl.col("_uuid_source"),
                plh.col("_uuid_source").uuidhash.uuid5(_DNS_NAMESPACE).alias("_foreign_uuid"),
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
                                pl.col("multiverseIds").list.first().cast(pl.String).alias("multiverseId"),
                                pl.col("id").alias("scryfallId"),
                            ]
                        ).alias("identifiers"),
                        pl.col("language"),
                        pl.col("multiverseIds").list.first().alias("multiverseId"),
                        pl.col("_foreign_name").alias("name"),
                        pl.col("_foreign_text").alias("text"),
                        pl.col("_foreign_type").alias("type"),
                        pl.col("_foreign_uuid").alias("uuid"),
                    ]
                ).alias("foreignData")
            )
            .with_columns(pl.col("foreignData").list.eval(pl.element().sort_by(pl.element().struct.field("language"))))
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

        spellbook_raw = self.spellbook_lf
        if spellbook_raw is not None:
            if isinstance(spellbook_raw, pl.LazyFrame):
                spellbook: pl.DataFrame = spellbook_raw.collect()
            else:
                spellbook = spellbook_raw
            if spellbook.height > 0:
                frames.append(("spellbook", spellbook))
                LOGGER.info(f"name: +spellbook ({spellbook.height:,} rows)")

        meld_triplets = self.meld_triplets
        if meld_triplets:
            meld_records = [{"name": name, "cardParts": parts} for name, parts in meld_triplets.items()]
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
        LOGGER.info(f"signatures_lf: {result.height:,} rows x {len(result.columns)} cols")

    def _build_watermark_overrides_lookup(self) -> None:
        """
        Build watermark overrides lookup (by setCode + name).

        For cards with watermark 'set', this provides the enhanced watermark
        like 'set (LEA)' or 'set (THS)' from the resource file.
        """
        watermarks_raw = self.set_code_watermarks
        if not watermarks_raw:
            LOGGER.info("watermark_overrides: No data found")
            return

        records = []
        for set_code, cards in watermarks_raw.items():
            for card in cards:
                for name_part in card["name"].split(" // "):
                    records.append(
                        {
                            "setCode": set_code.upper(),
                            "name": name_part,
                            "_watermarkOverride": card["watermark"],
                        }
                    )

        if not records:
            LOGGER.info("watermark_overrides: No overrides found")
            return

        result = pl.DataFrame(records)
        self.watermark_overrides_lf = result.lazy()
        LOGGER.info(f"watermark_overrides_lf: {result.height:,} rows x {len(result.columns)} cols")

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
            records.append(
                {
                    "scryfallId": scryfall_id,
                    "side": side,
                    "_faceFlavorName": data.get("faceFlavorName"),
                    "_flavorNameOverride": data.get("flavorName"),
                }
            )

        if not records:
            LOGGER.info("face_flavor_names: No data found")
            return

        self.face_flavor_names_df = pl.DataFrame(records)
        LOGGER.info(f"face_flavor_names_df: {self.face_flavor_names_df.height:,} rows")

    def _build_mcm_lookup(self) -> None:
        """
        Build MCM lookup with setCode mapping.

        Matches legacy behavior:
        - Direct match: MCM expansion name == Scryfall set name
        - Extras match: MCM expansion name == Scryfall set name + ": extras"
        - Applies mkm_set_name_fixes.json to remap MCM names to Scryfall names
        """
        mcm_raw = self.mcm_lookup_lf
        sets_raw = self.sets_lf

        if mcm_raw is None or sets_raw is None:
            LOGGER.info("mcm_lookup: Missing MCM or sets data, skipping")
            return

        mcm_df = mcm_raw.collect() if isinstance(mcm_raw, pl.LazyFrame) else mcm_raw
        sets_df = sets_raw.collect() if isinstance(sets_raw, pl.LazyFrame) else sets_raw

        if mcm_df.is_empty():
            LOGGER.info("mcm_lookup: MCM data is empty, skipping")
            return

        if "setCode" in mcm_df.columns:
            LOGGER.info("mcm_lookup: Already mapped, skipping")
            return

        mcm_fixes: dict[str, str] = {}
        if self.resource_path:
            fixes_path = self.resource_path / "mkm_set_name_fixes.json"
            if fixes_path.exists():
                with fixes_path.open(encoding="utf-8") as f:
                    mcm_fixes = json.load(f)

        base_mapping = sets_df.select(
            [
                pl.col("code").alias("setCode"),
                pl.col("name").str.to_lowercase().alias("_mcm_name"),
            ]
        )
        extras_mapping = sets_df.select(
            [
                pl.col("code").alias("setCode"),
                (pl.col("name").str.to_lowercase() + ": extras").alias("_mcm_name"),
            ]
        )
        set_mapping = pl.concat([base_mapping, extras_mapping]).unique(subset=["_mcm_name"])

        def apply_mcm_fixes(exp_name: str) -> str:
            lower = exp_name.lower()
            return mcm_fixes.get(lower, lower)

        result = (
            mcm_df.with_columns(
                pl.col("expansionName").map_elements(apply_mcm_fixes, return_dtype=pl.String).alias("_exp_name_fixed")
            )
            .join(
                set_mapping,
                left_on="_exp_name_fixed",
                right_on="_mcm_name",
                how="inner",
            )
            .with_columns(
                pl.col("name").str.to_lowercase().str.replace(r"\s*\(v\.\d+\)\s*$", "").alias("nameLower"),
            )
            .select(
                [
                    pl.col("mcmId"),
                    pl.col("mcmMetaId"),
                    pl.col("setCode"),
                    pl.col("nameLower"),
                    pl.col("number"),
                ]
            )
            .unique(subset=["setCode", "nameLower", "number"], keep="first")
        )
        self._mcm_lookup_enriched = result.lazy()

    def _build_mcm_set_map(self) -> None:
        """
        Build MCM set map for set metadata (set name -> mcmId/mcmName)
        """
        from mtgjson5 import constants

        raw_mcm_path = constants.CACHE_PATH / "mkm_cards.parquet"
        if not raw_mcm_path.exists():
            LOGGER.info("mcm_set_map: No raw MCM cache file, skipping")
            return

        try:
            mcm_df = pl.read_parquet(raw_mcm_path)
        except Exception as e:
            LOGGER.warning(f"mcm_set_map: Failed to read MCM cache: {e}")
            return

        if mcm_df.is_empty():
            LOGGER.info("mcm_set_map: MCM data is empty, skipping")
            return

        if "expansionId" not in mcm_df.columns or "expansionName" not in mcm_df.columns:
            LOGGER.info("mcm_set_map: MCM cache missing expansion columns, skipping")
            return

        expansions_df = (
            mcm_df.select(["expansionId", "expansionName"]).unique().filter(pl.col("expansionId").is_not_null())
        )

        if expansions_df.is_empty():
            LOGGER.info("mcm_set_map: No expansions found in MCM data")
            return

        mcm_fixes: dict[str, str] = {}
        if self.resource_path:
            fixes_path = self.resource_path / "mkm_set_name_fixes.json"
            if fixes_path.exists():
                with fixes_path.open(encoding="utf-8") as f:
                    mcm_fixes = json.load(f)

        raw_map: dict[str, dict[str, Any]] = {}
        for row in expansions_df.iter_rows(named=True):
            exp_name = row["expansionName"]
            exp_id = row["expansionId"]
            raw_map[exp_name.lower()] = {
                "mcmId": exp_id,
                "mcmName": exp_name,
            }

        for old_name, new_name in mcm_fixes.items():
            old_key = old_name.lower()
            if old_key in raw_map:
                self.mcm_set_map[new_name.lower()] = raw_map.pop(old_key)

        self.mcm_set_map.update(raw_map)
