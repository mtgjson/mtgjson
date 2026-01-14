"""
MTGJSON sealed product and booster models.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, Field

from mtgjson5.consts import ALLOW_IF_FALSEY

from .base import PolarsMixin
from .submodels import (
	BoosterConfig,
	BoosterPack,
	BoosterSheet,
	Identifiers,
	PurchaseUrls,
	SealedProductCard,
	SealedProductContents,
	SealedProductDeck,
	SealedProductOther,
	SealedProductPack,
	SealedProductSealed,
)


if TYPE_CHECKING:
	from polars import DataFrame


# =============================================================================
# Sealed Product Model
# =============================================================================


class SealedProduct(PolarsMixin, BaseModel):
	"""Sealed product (booster box, bundle, etc.)."""

	model_config = {"populate_by_name": True}

	# Override to exclude 'language' from required fields
	# SealedProduct only includes language for non-English products
	# Always include 'identifiers' even when empty {} (CDN behavior)
	_allow_if_falsey: ClassVar[frozenset[str]] = (ALLOW_IF_FALSEY - {"language"}) | {"identifiers"}

	uuid: str
	name: str
	category: str | None = None
	subtype: str | None = None
	language: str | None = None
	release_date: str | None = Field(default=None, alias="releaseDate")
	card_count: int | None = Field(default=None, alias="cardCount")
	product_size: int | None = Field(default=None, alias="productSize")
	contents: SealedProductContents | None = None
	identifiers: Identifiers = Field(default_factory=dict)
	purchase_urls: PurchaseUrls = Field(default_factory=dict, alias="purchaseUrls")


# =============================================================================
# Sealed Product Assembler
# =============================================================================


class SealedProductAssembler:
	"""Assembles SealedProduct objects from DataFrames."""

	@staticmethod
	def from_dataframe(df: DataFrame) -> list[dict[str, Any]]:
		"""Convert sealed products DataFrame to list of dicts."""
		models = SealedProduct.from_dataframe(df)
		return [m.to_polars_dict(exclude_none=True) for m in models]

	@staticmethod
	def assemble_contents(
		contents_df: DataFrame,
		product_uuid: str,
	) -> SealedProductContents:
		"""
		Assemble contents struct for a single product.

		Args:
		    contents_df: Contents DataFrame with contentType column
		    product_uuid: Product UUID to filter by

		Returns:
		    SealedProductContents TypedDict
		"""
		try:
			import polars as pl
		except ImportError as err:
			raise ImportError("Polars required") from err

		filtered = contents_df.filter(pl.col("productUuid") == product_uuid)

		contents: SealedProductContents = {}

		for content_type in ["card", "deck", "other", "pack", "sealed"]:
			type_rows = filtered.filter(pl.col("contentType") == content_type)
			if len(type_rows) == 0:
				continue

			if content_type == "card":
				contents["card"] = [
					SealedProductCard(
						uuid=r["uuid"],
						name=r["name"],
						number=r["number"],
						set=r["set"],
						foil=r.get("foil"),
					)
					for r in type_rows.to_dicts()
				]
			elif content_type == "sealed":
				contents["sealed"] = [
					SealedProductSealed(
						uuid=r["uuid"],
						name=r["name"],
						set=r["set"],
						count=r.get("count", 1),
					)
					for r in type_rows.to_dicts()
				]
			elif content_type == "deck":
				contents["deck"] = [SealedProductDeck(name=r["name"], set=r["set"]) for r in type_rows.to_dicts()]
			elif content_type == "pack":
				contents["pack"] = [SealedProductPack(code=r["code"], set=r["set"]) for r in type_rows.to_dicts()]
			elif content_type == "other":
				contents["other"] = [SealedProductOther(name=r["name"]) for r in type_rows.to_dicts()]

		return contents


# =============================================================================
# Booster Assembler
# =============================================================================


class BoosterAssembler:
	"""Assembles booster configurations from card/sheet data."""

	def __init__(self, cards_df: DataFrame):
		"""
		Args:
		    cards_df: DataFrame with card data including boosterTypes, uuid
		"""
		self.cards_df = cards_df

	def build_sheet(
		self,
		sheet_name: str,
		card_uuids: list[str],
		weights: list[int] | None = None,
		foil: bool = False,
		balance_colors: bool = False,
		allow_duplicates: bool = False,
		fixed: bool = False,
	) -> BoosterSheet:
		"""
		Build a single booster sheet.

		Args:
		    sheet_name: Name of the sheet
		    card_uuids: List of card UUIDs in the sheet
		    weights: Optional weights per card (defaults to 1 each)
		    foil: Whether cards from this sheet are foil
		    balance_colors: Whether to balance colors when picking
		    allow_duplicates: Whether duplicates are allowed
		    fixed: Whether this is a fixed sheet

		Returns:
		    BoosterSheet TypedDict
		"""
		if weights is None:
			weights = [1] * len(card_uuids)

		cards = dict(zip(card_uuids, weights, strict=False))
		total_weight = sum(weights)

		sheet: BoosterSheet = {
			"cards": cards,
			"foil": foil,
			"totalWeight": total_weight,
		}

		if allow_duplicates:
			sheet["allowDuplicates"] = True
		if balance_colors:
			sheet["balanceColors"] = True
		if fixed:
			sheet["fixed"] = True

		return sheet

	def build_sheet_from_df(
		self,
		sheet_name: str,
		filter_expr: Any,  # pl.Expr
		foil: bool = False,
		weight_col: str | None = None,
		**kwargs: Any,
	) -> BoosterSheet:
		"""
		Build a sheet from a filtered DataFrame.

		Args:
		    sheet_name: Name of the sheet
		    filter_expr: Polars filter expression
		    foil: Whether cards are foil
		    weight_col: Optional column for weights
		    **kwargs: Additional sheet options

		Returns:
		    BoosterSheet TypedDict
		"""
		filtered = self.cards_df.filter(filter_expr)

		uuids = filtered["uuid"].to_list()
		weights = None
		if weight_col and weight_col in filtered.columns:
			weights = filtered[weight_col].to_list()

		return self.build_sheet(sheet_name, uuids, weights, foil=foil, **kwargs)

	def build_pack(
		self,
		contents: dict[str, int],
		weight: int = 1,
	) -> BoosterPack:
		"""
		Build a booster pack configuration.

		Args:
		    contents: Mapping of sheet_name -> count
		    weight: Weight of this pack variant

		Returns:
		    BoosterPack TypedDict
		"""
		return BoosterPack(contents=contents, weight=weight)

	def build_config(
		self,
		sheets: dict[str, BoosterSheet],
		packs: list[BoosterPack],
		source_set_codes: list[str],
		name: str | None = None,
	) -> BoosterConfig:
		"""
		Build a complete booster configuration.

		Args:
		    sheets: Mapping of sheet_name -> BoosterSheet
		    packs: List of pack configurations
		    source_set_codes: Set codes that cards come from
		    name: Optional name for this booster type

		Returns:
		    BoosterConfig TypedDict
		"""
		total_weight = sum(p["weight"] for p in packs)

		config: BoosterConfig = {
			"boosters": packs,
			"boostersTotalWeight": total_weight,
			"sheets": sheets,
			"sourceSetCodes": sorted(source_set_codes),
		}

		if name:
			config["name"] = name

		return config

	@classmethod
	def build_draft_booster(
		cls,
		cards_df: DataFrame,
		set_code: str,
	) -> BoosterConfig:
		"""
		Build a standard draft booster configuration.

		Typical structure:
		- 1 rare/mythic
		- 3 uncommons
		- 10 commons
		- 1 basic land (or foil)

		Args:
		    cards_df: DataFrame with cards for the set
		    set_code: Set code

		Returns:
		    BoosterConfig for draft boosters
		"""
		try:
			import polars as pl
		except ImportError as err:
			raise ImportError("Polars required") from err

		assembler = cls(cards_df)

		# Build sheets by rarity
		sheets: dict[str, BoosterSheet] = {}

		# Mythics (weight 1) and rares (weight 2) combined
		mythics = cards_df.filter(pl.col("rarity") == "mythic")["uuid"].to_list()
		rares = cards_df.filter(pl.col("rarity") == "rare")["uuid"].to_list()

		rare_mythic_cards = {uuid: 1 for uuid in mythics}
		rare_mythic_cards.update({uuid: 2 for uuid in rares})

		if rare_mythic_cards:
			sheets["rareMythic"] = {
				"cards": rare_mythic_cards,
				"foil": False,
				"totalWeight": sum(rare_mythic_cards.values()),
			}

		# Uncommons
		uncommons = cards_df.filter(pl.col("rarity") == "uncommon")["uuid"].to_list()
		if uncommons:
			sheets["uncommon"] = assembler.build_sheet("uncommon", uncommons)

		# Commons
		commons = cards_df.filter(pl.col("rarity") == "common")["uuid"].to_list()
		if commons:
			sheets["common"] = assembler.build_sheet("common", commons)

		# Basic lands
		basics = cards_df.filter(pl.col("supertypes").list.contains("Basic") & pl.col("types").list.contains("Land"))[
			"uuid"
		].to_list()
		if basics:
			sheets["basicLand"] = assembler.build_sheet("basicLand", basics)

		# Standard pack: 1R/M, 3U, 10C, 1L
		pack_contents = {}
		if "rareMythic" in sheets:
			pack_contents["rareMythic"] = 1
		if "uncommon" in sheets:
			pack_contents["uncommon"] = 3
		if "common" in sheets:
			pack_contents["common"] = 10
		if "basicLand" in sheets:
			pack_contents["basicLand"] = 1

		packs = [assembler.build_pack(pack_contents)]

		return assembler.build_config(
			sheets=sheets,
			packs=packs,
			source_set_codes=[set_code],
			name="draft",
		)

	@staticmethod
	def from_json(data: dict[str, Any]) -> dict[str, BoosterConfig]:
		"""
		Parse booster configs from JSON (e.g., from set data).

		Args:
		    data: Booster data dict from set JSON

		Returns:
		    Dict of booster_type -> BoosterConfig
		"""
		# Already in correct format, just validate structure
		result: dict[str, BoosterConfig] = {}

		for booster_type, config in data.items():
			result[booster_type] = BoosterConfig(
				boosters=config.get("boosters", []),
				boostersTotalWeight=config.get("boostersTotalWeight", 0),
				sheets=config.get("sheets", {}),
				sourceSetCodes=config.get("sourceSetCodes", []),
				name=config.get("name"),
			)

		return result


# =============================================================================
# Namespace for Sealed Models
# =============================================================================


class Sealed:
	"""Namespace for all sealed product models."""

	SealedProduct = SealedProduct


# =============================================================================
# Registry for TypeScript generation
# =============================================================================

SEALED_MODEL_REGISTRY: list[type[BaseModel]] = [
	SealedProduct,
]

__all__ = [
	"SEALED_MODEL_REGISTRY",
	"Sealed",
]
