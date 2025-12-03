from dataclasses import dataclass, field
import polars as pl

from mtgjson5.cache import GLOBAL_CACHE


TOKEN_LAYOUTS={"token", "double_faced_token", "emblem", "art_series"}

@dataclass
class PipelineContext:
    """
    Container for all lookup data needed by the card pipeline.
    """

    # Core DataFrames
    cards_df: pl.LazyFrame|None = None
    sets_df: pl.DataFrame|None = None

    # Lookup DataFrames
    card_kingdom_df: pl.DataFrame|None = None
    mcm_lookup_df: pl.DataFrame|None = None
    printings_df: pl.DataFrame|None = None
    rulings_df: pl.DataFrame|None = None
    salt_df: pl.DataFrame|None = None
    spellbook_df: pl.DataFrame|None = None
    sld_subsets_df: pl.DataFrame|None = None
    uuid_cache_df: pl.DataFrame|None = None

    # Dict lookups
    gatherer_map: dict = field(default_factory=dict)
    meld_triplets: dict = field(default_factory=dict)
    manual_overrides: dict = field(default_factory=dict)
    multiverse_bridge_cards: dict = field(default_factory=dict)

    # Provider accessors
    standard_legal_sets: set[str] = field(default_factory=set)
    unlimited_cards: set[str] = field(default_factory=set)

    # GitHub data
    card_to_products_df: pl.DataFrame|None = None
   
   
    @classmethod
    def from_global_cache(cls) -> "PipelineContext":
        """Create a PipelineContext from the global cache."""
        return cls(
            cards_df=GLOBAL_CACHE.cards_df,
            sets_df=GLOBAL_CACHE.sets_df,
            card_kingdom_df=GLOBAL_CACHE.card_kingdom_df,
            mcm_lookup_df=GLOBAL_CACHE.mcm_lookup_df,
            printings_df=GLOBAL_CACHE.printings_df,
            rulings_df=GLOBAL_CACHE.rulings_df,
            salt_df=GLOBAL_CACHE.salt_df,
            spellbook_df=GLOBAL_CACHE.spellbook_df,
            sld_subsets_df=GLOBAL_CACHE.sld_subsets_df,
            uuid_cache_df=GLOBAL_CACHE.uuid_cache_df,
            gatherer_map=GLOBAL_CACHE.gatherer_map,
            meld_triplets=GLOBAL_CACHE.meld_triplets,
            manual_overrides=GLOBAL_CACHE.manual_overrides,
            multiverse_bridge_cards=GLOBAL_CACHE.multiverse_bridge_cards,
            standard_legal_sets=GLOBAL_CACHE.standard_legal_sets,
            unlimited_cards=GLOBAL_CACHE.scryfall.cards_without_limits
            if GLOBAL_CACHE._scryfall
            else set(),
            categoricals=GLOBAL_CACHE.categoricals,
            card_to_products_df=GLOBAL_CACHE.github.card_to_products_df
            if GLOBAL_CACHE._github
            else None,
        )

 
def _ascii_name_expr(expr: pl.Expr) -> pl.Expr:
    """
    Build expression to normalize card name to ASCII.
    Pure Polars - stays lazy.
    """
    return (
        expr.str.replace_all("Æ", "AE")
        .str.replace_all("æ", "ae")
        .str.replace_all("Œ", "OE")
        .str.replace_all("œ", "oe")
        .str.replace_all("ß", "ss")
        .str.replace_all("É", "E")
        .str.replace_all("È", "E")
        .str.replace_all("Ê", "E")
        .str.replace_all("Ë", "E")
        .str.replace_all("Á", "A")
        .str.replace_all("À", "A")
        .str.replace_all("Â", "A")
        .str.replace_all("Ä", "A")
        .str.replace_all("Ã", "A")
        .str.replace_all("Í", "I")
        .str.replace_all("Ì", "I")
        .str.replace_all("Î", "I")
        .str.replace_all("Ï", "I")
        .str.replace_all("Ó", "O")
        .str.replace_all("Ò", "O")
        .str.replace_all("Ô", "O")
        .str.replace_all("Ö", "O")
        .str.replace_all("Õ", "O")
        .str.replace_all("Ú", "U")
        .str.replace_all("Ù", "U")
        .str.replace_all("Û", "U")
        .str.replace_all("Ü", "U")
        .str.replace_all("Ý", "Y")
        .str.replace_all("Ñ", "N")
        .str.replace_all("Ç", "C")
        .str.replace_all("é", "e")
        .str.replace_all("è", "e")
        .str.replace_all("ê", "e")
        .str.replace_all("ë", "e")
        .str.replace_all("á", "a")
        .str.replace_all("à", "a")
        .str.replace_all("â", "a")
        .str.replace_all("ä", "a")
        .str.replace_all("ã", "a")
        .str.replace_all("í", "i")
        .str.replace_all("ì", "i")
        .str.replace_all("î", "i")
        .str.replace_all("ï", "i")
        .str.replace_all("ó", "o")
        .str.replace_all("ò", "o")
        .str.replace_all("ô", "o")
        .str.replace_all("ö", "o")
        .str.replace_all("õ", "o")
        .str.replace_all("ú", "u")
        .str.replace_all("ù", "u")
        .str.replace_all("û", "u")
        .str.replace_all("ü", "u")
        .str.replace_all("ý", "y")
        .str.replace_all("ÿ", "y")
        .str.replace_all("ñ", "n")
        .str.replace_all("ç", "c")
    )
    

def is_token_expr() -> pl.Expr:
    """
    Expression to detect if a row is a token based on layout/type.

    Returns:
        pl.Expr: Boolean expression for token detection.
    """
    return (
        pl.col("layout").is_in(TOKEN_LAYOUTS)
        | (pl.col("type_line").fill_null("") == "Dungeon")
        | pl.col("type_line").fill_null("").str.contains("Token")
    )
    
    
def mark_tokens(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add _isToken boolean column to identify tokens.

    Should be called early in the pipeline so conditional expressions can use it.
    """
    return lf.with_columns(is_token_expr().alias("_isToken"))


def token_conditional(
    card_expr: pl.Expr,
    token_expr: pl.Expr | None = None,
    alias: str | None = None,
) -> pl.Expr:
    """
    Return card_expr for cards, token_expr (or null) for tokens.

    Usage:
        .with_columns(
            token_conditional(legalities_struct, alias="legalities"),  # null for tokens
            token_conditional(pl.lit(None), reverse_related_expr, alias="reverseRelated"),  # only for tokens
        )
    """
    expr = pl.when(~pl.col("_isToken")).then(card_expr)
    if token_expr is not None:
        expr = expr.otherwise(token_expr)
    else:
        expr = expr.otherwise(pl.lit(None))
    if alias:
        expr = expr.alias(alias)
    return expr
