"""
Vectorized Polars expressions for card data transformations.

Replaces map_elements UDFs with pure Polars expressions for
significant performance improvements.
"""

from __future__ import annotations

import polars as pl
import polars_hash as plh

from mtgjson5.consts.mappings import ASCII_REPLACEMENTS


def uuid5_expr(col_name: str) -> pl.Expr:
	"""Generate UUID5 from a column name using DNS namespace."""
	return plh.col(col_name).uuidhash.uuid5()


def uuid5_concat_expr(col1: pl.Expr, col2: pl.Expr, default: str = "a") -> pl.Expr:
	"""Generate UUID5 from concatenation of two columns."""
	return plh.col(col1.meta.output_name()).uuidhash.uuid5_concat(col2, default=default)


def ascii_name_expr(col: str | pl.Expr) -> pl.Expr:
	"""
	Normalize card name to ASCII.

	Uses str.replace_many for efficient batch replacement.
	"""
	expr = pl.col(col) if isinstance(col, str) else col
	return expr.str.replace_many(ASCII_REPLACEMENTS)


def order_finishes_expr(col: str = "finishes") -> pl.Expr:
	"""
	Orders finishes without Python UDFs.
	Assigns weights: nonfoil=0, foil=1, etched=2, signed=3, others=99.
	"""
	return pl.col(col).list.eval(
		pl.element().sort_by(
			pl.element().replace_strict(
				{"nonfoil": 0, "foil": 1, "etched": 2, "signed": 3}, default=99, return_dtype=pl.Int32
			),
			pl.element(),  # secondary sort alphabetical
		)
	)


def calculate_cmc_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
	"""
	Pure Polars CMC calculation. str.replace uses rust regex under the hood.

	Handles: numbers, colors (WUBRGCEP), hybrid (2/W, W/P), half (H), variable (XYZ).
	"""
	expr = pl.col(col) if isinstance(col, str) else col

	# 1. Extract everything between {}
	# 2. Strip braces (extract_all returns full matches like "{2}", not capture groups)
	# 3. Handle half-mana (H), hybrid (2/W), variable (X), and colors
	# 4. Sum the results
	return (
		expr.fill_null("")
		.str.extract_all(r"\{([^}]+)\}")
		.list.eval(
			pl.element()
			.str.strip_chars("{}")  # Remove braces from full match
			.str.replace_all(r"^[XYZ]$", "0")  # Variable mana -> 0
			.str.replace_all(r"^H.*", "0.5")  # Half mana -> 0.5
			.str.replace_all(r"^(\d+)/.*$", "$1")  # Hybrid 2/W -> 2
			.str.replace_all(r"^[WUBRGCEP]/.*$", "1")  # Hybrid W/P -> 1
			.str.replace_all(r"^[WUBRGCEP]$", "1")  # Single color -> 1
			.cast(pl.Float64, strict=False)
			.fill_null(1.0)  # Fallback for unknown symbols
		)
		.list.sum()
		.fill_null(0.0)
		.abs()
	)


def extract_colors_from_mana_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
	"""
	Extract color letters from mana cost.

	Vectorized replacement for color extraction from mana cost.

	"{2}{W}{U}" -> ["W", "U"]
	"{2/W}{G}" -> ["W", "G"]

	Args:
	    col: Name of mana cost column or expression

	Returns:
	    Expression producing list of color letters sorted in WUBRG order
	"""
	expr = pl.col(col) if isinstance(col, str) else col
	return (
		expr.fill_null("")
		# Extract all WUBRG characters from the string
		.str.extract_all(r"[WUBRG]")
		.list.unique()
		.list.eval(pl.element().replace_strict({"W": 0, "U": 1, "B": 2, "R": 3, "G": 4}, return_dtype=pl.Int8))
		.list.sort()
		.list.eval(pl.element().replace_strict({0: "W", 1: "U", 2: "B", 3: "R", 4: "G"}, return_dtype=pl.String))
	)


def sort_colors_wubrg_expr(col: str | pl.Expr = "colors") -> pl.Expr:
	"""
	Sort a colors list in WUBRG order.

	Args:
	    col: Name of colors column or expression

	Returns:
	    Expression producing list of color letters sorted in WUBRG order
	"""
	expr = pl.col(col) if isinstance(col, str) else col
	return (
		expr.fill_null([])
		.list.eval(pl.element().replace_strict({"W": 0, "U": 1, "B": 2, "R": 3, "G": 4}, return_dtype=pl.Int8))
		.list.sort()
		.list.eval(pl.element().replace_strict({0: "W", 1: "U", 2: "B", 3: "R", 4: "G"}, return_dtype=pl.String))
	)


__all__ = [
	"ascii_name_expr",
	"calculate_cmc_expr",
	"extract_colors_from_mana_expr",
	"order_finishes_expr",
	"sort_colors_wubrg_expr",
	"uuid5_concat_expr",
	"uuid5_expr"
]
