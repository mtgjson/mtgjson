"""
Vectorized Polars expressions for card data transformations.

Replaces map_elements UDFs with pure Polars expressions for
significant performance improvements.
"""

from __future__ import annotations

import polars as pl


# =============================================================================
# Finish Ordering
# =============================================================================

def order_finishes_expr(col: str = "finishes") -> pl.Expr:
    """
    Orders finishes without Python UDFs.
    Assigns weights: nonfoil=0, foil=1, etched=2, signed=3, others=99.
    """
    return (
        pl.col(col)
        .list.eval(
            pl.element().sort_by(
                pl.element().replace_strict(
                    {"nonfoil": 0, "foil": 1, "etched": 2, "signed": 3},
                    default=99,
                    return_dtype=pl.Int32
                ),
                pl.element() # secondary sort alphabetical
            )
        )
    )



# =============================================================================
# Mana Cost Parsing
# =============================================================================


def calculate_cmc_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
    """
    Pure Polars CMC calculation. str.replace uses rust regex under the hood.
    """
    expr = pl.col(col) if isinstance(col, str) else col
    
    # 1. Extract everything between {}
    # 2. Handle half-mana (H), hybrid (2/W), and digits
    # 3. Sum the results
    return (
        expr.fill_null("")
        .str.extract_all(r"\{([^}]+)\}")
        .list.eval(
            # keep everything as string for replacements
            pl.element().replace_strict({"X": "0", "Y": "0", "Z": "0"}, default=pl.element())
            .str.replace(r"^H.*", "0.5") # Half mana
            .str.replace(r"(\d+)/.*", r"\1") # Hybrid 2/W -> 2
            .str.replace(r"[WUBRGCEP]/.*", "1") # Hybrid W/P -> 1
            .str.replace(r"[WUBRGCEP]", "1") # Colors -> 1
            .cast(pl.Float64, strict=False)
            .fill_null(1.0) # Fallback for unknown symbols
        )
        .list.sum()
        .fill_null(0.0)
    )


def extract_colors_from_mana_expr(col: str | pl.Expr = "manaCost") -> pl.Expr:
    """
    Extract color letters from mana cost.

    Vectorized replacement for color extraction from mana cost.

    "{2}{W}{U}" -> ["U", "W"]
    "{2/W}{G}" -> ["G", "W"]

    Args:
        col: Name of mana cost column or expression

    Returns:
        Expression producing sorted list of color letters
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return (
        expr.fill_null("")
        # Extract all WUBRG characters from the string
        .str.extract_all(r"[WUBRG]")
        .list.unique()
        .list.sort()
    )

__all__ = [
    "calculate_cmc_expr",
    "extract_colors_from_mana_expr",
    "order_finishes_expr",
]
