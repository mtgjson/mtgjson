"""Sealed content compilation — inline replacements for mtg-sealed-content outputs."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import ijson
import polars as pl
import polars_hash as plh
import yaml
from mtg_sealed_choices.catalog import CatalogWalker
from mtg_sealed_choices.links import (
    CardReference as _CTPCard,
)
from mtg_sealed_choices.links import (
    results_to_json as _ctp_results_to_json,
)
from mtg_sealed_choices.model import Card, Deck, Other, Pack, Product, Sealed, deck_links
from mtg_sealed_choices.uuid_map import uuid_map_from_events

from mtgjson5.pipeline.stages.explode import _uuid5_concat_expr
from mtgjson5.pipeline.stages.sealed_uuids import name_uuid, resolve_sealed_uuids

LOGGER = logging.getLogger(__name__)


def compile_products(products_dir: Path) -> dict:
    """Compile products.json from YAML source files.

    Replicates: mtg-sealed-content/scripts/new_products_compiler.py

    A product's nested ``contents`` (merged layout, see compile_contents) is
    dropped. Contents are compiled on their own, and everything that reads
    this dict -- the sealed products frame, the UUID pins,
    build_pipeline_view -- expects the plain product definition.

    Args:
        products_dir: Path to directory containing per-set product YAML files.

    Returns:
        Dict keyed by set code, values are product dicts.
    """
    result: dict = {}
    for file in sorted(products_dir.glob("*.yaml")):
        data = yaml.safe_load(file.read_bytes())
        code = data["code"]
        products = data["products"]
        for info in products.values():
            if isinstance(info, dict):
                info.pop("contents", None)
        LOGGER.debug("Loaded %d products for %s from %s", len(products), code, file.name)
        result[code] = products
    LOGGER.info("Compiled products for %d sets", len(result))
    return result


class card(Card):
    def __init__(self, contents: dict) -> None:
        super().__init__(contents)
        self.language: str | None = contents.get("language")

    def toJson(self) -> dict:
        data = super().toJson()
        if self.language:
            data["language"] = self.language
        return data

    def get_uuids(self, uuid_map: dict) -> None:
        try:
            set_map = uuid_map[self.set.lower()]
            number = str(self.number)
            # Tokens (e.g. SLD 918 "Food") are listed separately from the
            # regular cards, and the token flag picks which one to look in.
            primary = "tokens" if self.token else "cards"
            fallback = "cards" if self.token else "tokens"

            entry = None
            if self.language and not self.token:
                # Scryfall's all_cards data carries one row per (set, number,
                # language) -- e.g. WAR:5 "Battlefield Promotion" has a
                # distinct card object and UUID for every language it was
                # printed in. build_uuid_map_from_pipeline() indexes those
                # under "cards_by_language" for any (set, number) where more
                # than one language exists, so an explicit language picks the
                # matching printing instead of always falling through to the
                # single default entry in "cards" (English when available,
                # otherwise whichever language exists).
                #
                # Tokens are excluded on purpose. A per-language UUID is a
                # foreignData UUID, and tokens carry no foreignData at all
                # (CardToken extends CardPrintingBase, while foreign_data is
                # declared on CardAtomicBase), so there is nothing for one to
                # resolve to -- the default entry is the only UUID a token
                # number actually has in the output.
                by_lang = set_map.get("cards_by_language", {}).get(number, {})
                if self.language in by_lang:
                    entry = by_lang[self.language]
                elif by_lang:
                    # Only a number that really was printed in several
                    # languages can be missing the requested one. A number
                    # printed in a single language carries no index entry at
                    # all, and a correct language tag on it resolves through
                    # the default map below -- warning there would fire on
                    # perfectly valid input.
                    LOGGER.warning(
                        "Card number %s:%s has no %s printing in set %s; using the default-language entry instead",
                        self.set,
                        self.number,
                        self.language,
                        self.set,
                    )

            if entry is None:
                if number in set_map.get(primary, {}):
                    entry = set_map[primary][number]
                else:
                    # Still resolve it, but report the mismatch so the flag can
                    # be corrected in mtg-sealed-content.
                    entry = set_map[fallback][number]
                    LOGGER.warning(
                        "Card number %s:%s found in %s, token flag should be %s",
                        self.set,
                        self.number,
                        fallback,
                        not self.token,
                    )
            self.uuid = entry[0]
            if self.name not in entry[1]:
                raise ValueError("name and number do not match", self.name, self.name)
        except KeyError:
            LOGGER.warning("Card number %s:%s not found in set %s", self.set, self.number, self.set)
            self.uuid = None
        except ValueError:
            LOGGER.warning(
                "Card number %s:%s not found with name %s",
                self.set,
                self.number,
                self.name,
            )
            self.uuid = None


class pack(Pack):
    def get_uuids(self, uuid_map: dict) -> None:
        try:
            umap = uuid_map[self.set.lower()]["booster"]
        except KeyError:
            umap = False
        if not umap or (self.code not in umap):
            LOGGER.warning("Booster code %s not found in set %s", self.code, self.set)


class deck(Deck):
    def get_uuids(self, uuid_map: dict) -> None:
        try:
            umap = uuid_map[self.set.lower()]["decks"]
        except KeyError:
            umap = False
        if not umap or (self.name not in umap):
            LOGGER.warning("Deck named %s not found in set %s", self.name, self.set)


class sealed(Sealed):
    def get_uuids(self, uuid_map: dict) -> None:
        try:
            self.uuid = uuid_map[self.set.lower()]["sealedProduct"][self.name]
        except KeyError:
            LOGGER.warning("Product name %s not found in set %s", self.name, self.set)
            self.uuid = None


class other(Other):
    pass


class product(Product):
    card_type = card
    pack_type = pack
    deck_type = deck
    sealed_type = sealed
    other_type = other

    def unknown_bonus(self) -> None:
        LOGGER.warning("Product name %s missing bonus card definition", self.name)

    def serialized_cards(self) -> list[dict]:
        # The published schema requires a UUID. Resolution already reports
        # missing cards; omit them so one bad entry cannot fail the whole set.
        resolved = [c for c in self.card if c.uuid]
        if len(resolved) != len(self.card):
            LOGGER.warning(
                "Product %s - %s: dropping %d unresolved card entries from contents",
                self.set_code,
                self.name,
                len(self.card) - len(resolved),
            )
        return [c.toJson() for c in resolved]

    def resolve_uuid(self, uuid_map: dict) -> None:
        if self.name:
            try:
                self.uuid = uuid_map[self.set_code.lower()]["sealedProduct"][self.name]  # type: ignore[union-attr]
            except KeyError:
                LOGGER.warning("Product name %s not found in set %s", self.name, self.set_code)
                self.uuid = None
        else:
            self.uuid = None


def build_uuid_map(allprintings_path: Path) -> dict:
    """Parse AllPrintings.json via streaming to build a UUID lookup map.

    Returns a dict keyed by lowercase set code with sub-keys:
        booster: set of booster type codes
        decks: set of deck names
        sealedProduct: {product_name: uuid}
        cards: {number_str: (uuid, name)}  — only side "a" cards
        tokens: {number_str: (uuid, name)}  — only side "a" tokens
    """
    LOGGER.info("Loading AllPrintings.json from %s ...", allprintings_path)
    with open(allprintings_path, "rb") as f:
        uuids = uuid_map_from_events(ijson.parse(f))

    LOGGER.info("Built UUID map for %d sets", len(uuids))
    return uuids


def _is_token_expr(cards_lf: pl.LazyFrame) -> pl.Expr:
    """Mirror filter_out_tokens() over raw Scryfall columns.

    output.filter_out_tokens() is what decides whether a card ends up in a
    set's cards[] or its tokens[], and a sealed-content card: entry carrying
    ``token: true`` is looked up in the latter. Checking layout == "token"
    alone missed everything else that lands in tokens[]: the double-faced
    tokens (TMID:19 "Day // Night", TCLB:20 "Undercity // The Initiative"),
    the emblems, the art series, and the AFR dungeons, which Scryfall ships
    with layout "normal" and only a "Dungeon" type line.

    The MTGJSON rename to "type" has not happened yet at this point, so the
    type checks run against Scryfall's "type_line" -- absent from some rows
    (and from lean test frames), hence the fill_null.
    """
    from mtgjson5.consts import TOKEN_LAYOUTS

    has_type_line = "type_line" in cards_lf.collect_schema()
    type_line = pl.col("type_line") if has_type_line else pl.lit(None, dtype=pl.Utf8)
    return (
        pl.col("layout").is_in(list(TOKEN_LAYOUTS))
        | (type_line == "Dungeon")
        | type_line.str.contains("Token")
        | (type_line == "Card")
    ).fill_null(False)


def build_uuid_map_from_pipeline(
    cards_lf: pl.LazyFrame,
    uuid_cache_lf: pl.LazyFrame | None,
    boosters_raw: dict,
    decks_raw: list,
    products_dict: dict,
    sets_lf: pl.LazyFrame | None = None,
) -> dict:
    """Build the same UUID lookup map as build_uuid_map(), but from pipeline LazyFrames.

    This eliminates the AllPrintings.json dependency for sealed compilation.

    Returns a dict keyed by lowercase set code with sub-keys:
        cards: {number_str: (uuid, name)}  — the default pick for a number:
            English when a printing at that number exists in English, else
            whichever language does. Used when a card: entry has no explicit
            language.
        tokens: {number_str: (uuid, name)}  — the subset MTGJSON publishes in
            the set's tokens[] rather than its cards[], identified with the
            same rule filter_out_tokens() uses. A set that keeps its tokens in
            a separate Scryfall set (TAFR for AFR, TCLB for CLB, ...) gets
            those folded in here under the parent code, because that is where
            assemble.load_set_tokens() puts them in the output and how
            mtg-sealed-content refers to them.
        cards_by_language: {number_str: {language: (uuid, name)}}  — every
            language a (set, number) was printed in, present only where more
            than one language exists for that number. For the default
            language this is the same UUID as "cards" above; for every other
            language it is that printing's foreignData UUID -- computed the
            same way _build_foreign_data_df() computes the one that ends up
            in AllPrintings.json's foreignData[].uuid, not the language's own
            Scryfall ID or a fresh hash of it (verified against a live
            AllPrintings.json: e.g. GRAVE PACT's Chinese Simplified/French/
            German/Italian/Japanese/Portuguese/Russian/Spanish foreignData
            UUIDs all reproduce exactly from its English printing's Scryfall
            ID this way). Used when a card: entry names a specific language.
            Token layouts are left out: they have no foreignData in the
            output, so no per-language UUID exists for them and a language-
            tagged token resolves through "tokens"/"cards" instead.
        booster: set of booster type codes
        decks: set of deck names
        sealedProduct: {product_name: uuid}
    """
    from mtgjson5.consts import LANGUAGE_MAP
    from mtgjson5.data.context import _DNS_NAMESPACE

    uuids: dict[str, dict] = {}

    # Token sets are their own Scryfall sets (TAFR, TCLB, TMID, ...) but
    # MTGJSON publishes their cards inside the parent set's tokens[], with the
    # parent's code -- see assemble.load_set_tokens(). mtg-sealed-content
    # follows the output, so its dungeon entries read "set: afr, number: 20,
    # token: true" and have to resolve against AFR even though the row lives
    # in TAFR here.
    token_set_parents: dict[str, str] = {}
    if sets_lf is not None:
        sets_df = sets_lf.collect() if isinstance(sets_lf, pl.LazyFrame) else sets_lf
        if {"code", "set_type", "parent_set_code"} <= set(sets_df.columns):
            token_sets = sets_df.filter(
                (pl.col("set_type") == "token") & pl.col("parent_set_code").is_not_null()
            ).select("code", "parent_set_code")
            token_set_parents = {
                row["code"].lower(): row["parent_set_code"].lower() for row in token_sets.iter_rows(named=True)
            }

    # all_cards.ndjson includes all languages: a (set, collector_number) that
    # was printed in English, German, Japanese, etc. has one row per language,
    # each with its own Scryfall ID (e.g. WAR:5 "Battlefield Promotion" — see
    # get_uuids() on the card class for why this matters). Compute every
    # row's UUID and human-readable language up front; the two indices below
    # (single default pick vs. full per-language map) are both built from it.
    cards_all_df = cards_lf.with_columns(
        pl.col("lang").replace_strict(LANGUAGE_MAP, default=pl.col("lang")).alias("language"),
        pl.when(pl.col("lang") == "en").then(0).otherwise(1).alias("_lang_rank"),
        _is_token_expr(cards_lf).alias("is_token"),
    ).select(
        pl.col("id").alias("scryfallId"),
        pl.col("set").alias("set_lower"),
        pl.col("collector_number").alias("number"),
        pl.col("name"),
        pl.col("is_token"),
        pl.col("language"),
        pl.col("_lang_rank"),
    )

    if uuid_cache_lf is not None:
        # Filter cache to side "a" only, then left-join
        cache_a = uuid_cache_lf.filter(pl.col("side") == "a").select("scryfallId", "cachedUuid")
        cards_all_df = cards_all_df.join(cache_a, on="scryfallId", how="left")
    else:
        cards_all_df = cards_all_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("cachedUuid"))

    # Add a literal side column for uuid5_concat (always "a" since we only want
    # front-face cards for the sealed UUID map).
    cards_all_df = cards_all_df.with_columns(pl.lit("a").alias("side"))

    # Compute MTGJSON UUID: coalesce(cachedUuid, uuid5(scryfallId || "a"))
    cards_all_df = cards_all_df.with_columns(
        pl.coalesce(
            pl.col("cachedUuid"),
            _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
        ).alias("uuid")
    )

    cards_all_collected = cards_all_df.collect()

    def _ensure_set(code: str) -> dict:
        if code not in uuids:
            uuids[code] = {
                "cards": {},
                "tokens": {},
                "cards_by_language": {},
                "booster": set(),
                "decks": set(),
                "sealedProduct": {},
            }
        return uuids[code]

    # Default pick: exactly one entry per (set, collector_number), preferring
    # English but falling back to whatever language exists for numbers that
    # only have a non-English printing (e.g. Phyrexian-language cards).
    cards_collected = cards_all_collected.sort(["set_lower", "number", "_lang_rank"]).unique(
        subset=["set_lower", "number"], keep="first"
    )

    for row in cards_collected.iter_rows(named=True):
        set_map = _ensure_set(row["set_lower"])
        entry = (row["uuid"], row["name"])
        set_map["cards"][row["number"]] = entry
        # Tokens printed inside a regular set (e.g. SLD 918 "Food") end up in
        # {set}.tokens rather than {set}.cards, and sealed contents flagged as
        # token are resolved against this map.
        if row["is_token"]:
            set_map["tokens"][row["number"]] = entry

    # Fold each token set's tokens into its parent, which is the only code a
    # sealed-content entry ever names for them. setdefault, so a token the
    # parent set printed itself at the same number stays the one that wins.
    for token_code, parent_code in token_set_parents.items():
        token_map = uuids.get(token_code, {}).get("tokens")
        if not token_map:
            continue
        parent_tokens = _ensure_set(parent_code)["tokens"]
        for number, entry in token_map.items():
            parent_tokens.setdefault(number, entry)

    # Per-language index, restricted to (set, number) pairs that were
    # actually printed in more than one language -- the common case (a
    # number with a single language, e.g. most tokens and language-exclusive
    # promos) needs no entry here and stays resolvable only through the
    # default map above.
    #
    # A non-default-language printing does NOT get its own UUID computed from
    # its own Scryfall ID: AllPrintings.json folds it into foreignData[] on
    # the default-language card, and _build_foreign_data_df() (mtgjson5/data/
    # context.py) derives that entry's UUID from the *default* printing's
    # Scryfall ID, its side ("a"), and the language name --
    # uuid5(defaultScryfallId + "a" + "_" + language, NAMESPACE_DNS) -- not
    # from the foreign printing's own Scryfall ID. Reproduce that exactly so
    # a language-tagged card: entry resolves to the same UUID that already
    # exists in foreignData[].uuid, instead of an unrelated one nothing else
    # in the dataset would recognize.
    multi_language_numbers = (
        cards_all_collected.group_by(["set_lower", "number"])
        .agg(pl.col("language").n_unique().alias("_language_count"))
        .filter(pl.col("_language_count") > 1)
        .select(["set_lower", "number"])
    )
    default_lookup = cards_collected.select(
        pl.col("set_lower"),
        pl.col("number"),
        pl.col("scryfallId").alias("_default_scryfall_id"),
        pl.col("language").alias("_default_language"),
        pl.col("uuid").alias("_default_uuid"),
    )
    by_language_rows = (
        cards_all_collected.filter(~pl.col("is_token"))
        .join(multi_language_numbers, on=["set_lower", "number"], how="inner")
        .join(default_lookup, on=["set_lower", "number"], how="left")
        .with_columns(
            pl.concat_str([pl.col("_default_scryfall_id"), pl.lit("a"), pl.lit("_"), pl.col("language")]).alias(
                "_foreign_uuid_source"
            )
        )
        .with_columns(plh.col("_foreign_uuid_source").uuidhash.uuid5(_DNS_NAMESPACE).alias("_foreign_uuid"))
        .with_columns(
            pl.when(pl.col("language") == pl.col("_default_language"))
            .then(pl.col("_default_uuid"))
            .otherwise(pl.col("_foreign_uuid"))
            .alias("_language_uuid")
        )
    )
    for row in by_language_rows.iter_rows(named=True):
        set_map = _ensure_set(row["set_lower"])
        entry = (row["_language_uuid"], row["name"])
        set_map["cards_by_language"].setdefault(row["number"], {})[row["language"]] = entry

    for set_code, booster_config in boosters_raw.items():
        code = set_code.lower()
        _ensure_set(code)
        uuids[code]["booster"] = set(booster_config.keys())

    for deck_entry in decks_raw:
        code = deck_entry["set_code"].lower()
        _ensure_set(code)
        uuids[code]["decks"].add(deck_entry["name"])

    for (set_code, product_name), puuid in resolve_sealed_uuids(products_dict).items():
        set_lower = set_code.lower()
        _ensure_set(set_lower)
        uuids[set_lower]["sealedProduct"][product_name] = puuid

    LOGGER.info("Built UUID map from pipeline for %d sets", len(uuids))
    return uuids


def build_card_finishes_lookup(
    cards_lf: pl.LazyFrame,
    uuid_cache_lf: pl.LazyFrame | None,
) -> dict[str, dict]:
    """Build {mtgjson_uuid: {"finishes": [...], "number": str, "set": str}} from Scryfall.

    Used by card_to_products compilation to determine card finish types.
    Includes both regular cards and tokens.

    Args:
        cards_lf: Scryfall cards LazyFrame (snake_case: id, set, collector_number,
                  name, finishes, layout, lang).
        uuid_cache_lf: UUID cache LazyFrame (scryfallId, side, cachedUuid).

    Returns:
        Dict keyed by MTGJSON UUID with values containing finishes list,
        collector number, and set code.
    """
    # Deduplicate by (set, collector_number), preferring English
    cards_df = (
        cards_lf.with_columns(pl.when(pl.col("lang") == "en").then(0).otherwise(1).alias("_lang_rank"))
        .sort("set", "collector_number", "_lang_rank")
        .unique(subset=["set", "collector_number"], keep="first")
        .select(
            pl.col("id").alias("scryfallId"),
            pl.col("set"),
            pl.col("collector_number"),
            pl.col("finishes"),
            pl.col("layout"),
        )
    )

    # Compute MTGJSON UUID
    if uuid_cache_lf is not None:
        cache_a = uuid_cache_lf.filter(pl.col("side") == "a").select("scryfallId", "cachedUuid")
        cards_df = cards_df.join(cache_a, on="scryfallId", how="left")
    else:
        cards_df = cards_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("cachedUuid"))

    cards_df = cards_df.with_columns(pl.lit("a").alias("side"))

    cards_df = cards_df.with_columns(
        pl.coalesce(
            pl.col("cachedUuid"),
            _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
        ).alias("uuid")
    )

    # Collect and build dict
    df = cards_df.collect()

    result: dict[str, dict] = {}
    for row in df.iter_rows(named=True):
        finishes = row["finishes"]
        if finishes is None:
            finishes = ["nonfoil"]
        result[row["uuid"]] = {
            "finishes": finishes,
            "number": row["collector_number"],
            "set": row["set"],
        }

    LOGGER.info("Built card finishes lookup with %d entries", len(result))
    return result


def set_to_json(set_content: dict) -> dict:
    """Serialize product objects and filter out empty results."""
    decoded = {k: v.toJson() for k, v in set_content.items()}
    return {k: v for k, v in decoded.items() if v}


def _iter_set_contents(products_dir: Path, contents_dir: Path | None) -> Iterator[tuple[str, dict]]:
    """Yield ``(set_code, {product_name: contents})`` for every set file.

    With a contents_dir (split layout) that is each data/contents/SET.yaml as
    is. Without one (merged layout) it is the ``contents`` nested under each
    product in data/products/SET.yaml, read back into the same shape. A
    product without that key has not been researched yet, which the split
    layout spells as an empty placeholder, so it maps to None.

    The values are the parsed YAML objects themselves, not copies, so a
    ``copy`` resolved against this mapping reaches its target's contents in
    exactly the way it reaches the target's data/contents/ entry.
    """
    if contents_dir is not None:
        for set_file in sorted(contents_dir.glob("*.yaml")):
            contents = yaml.safe_load(set_file.read_bytes())
            yield contents["code"], contents["products"]
        return

    for set_file in sorted(products_dir.glob("*.yaml")):
        data = yaml.safe_load(set_file.read_bytes())
        set_contents = {
            name: info.get("contents") if isinstance(info, dict) else None for name, info in data["products"].items()
        }
        yield data["code"], set_contents


def compile_contents(products_dir: Path, contents_dir: Path | None, uuid_map: dict) -> tuple[dict, dict]:
    """Compile contents.json and deck_map.json from YAML source files.

    Replicates: mtg-sealed-content/scripts/product_contents_compiler.py

    mtg-sealed-content is folding data/contents/SET.yaml into
    data/products/SET.yaml as a nested ``contents`` key, and both layouts
    compile to the same result. The caller decides the layout once for the
    whole tree (merged exactly when it has no data/contents/, see
    SealedDataProvider._fetch_and_extract_yaml), never per product.

    Args:
        products_dir: Path to directory containing per-set product YAML files.
        contents_dir: Path to directory containing per-set content YAML files,
            or None for the merged layout, where products_dir holds both.
        uuid_map: UUID lookup map from build_uuid_map().

    Returns:
        Tuple of (contents_dict, deck_map_dict).
    """
    products_contents: dict = {}

    for code, set_contents in _iter_set_contents(products_dir, contents_dir):
        products_contents[code] = {}

        for name, p in set_contents.items():
            if not p:
                LOGGER.warning("Product %s - %s missing contents", code, name)
                continue
            if set(p.keys()) == {"copy"}:
                p = set_contents[p["copy"]]
            compiled_product = product(p, code, name)
            compiled_product.get_uuids(uuid_map)
            products_contents[code][name] = compiled_product

        if not products_contents[code]:
            products_contents.pop(code)

    LOGGER.info("Compiled contents for %d sets", len(products_contents))

    contents_dict = {k: set_to_json(v) for k, v in products_contents.items()}
    deck_map_dict = deck_links(products_contents)
    return contents_dict, deck_map_dict


def build_pipeline_view(
    contents_dict: dict,
    boosters_raw: dict,
    decks_raw: list,
    card_finishes: dict,
    products_dict: dict,
    uuid_map: dict | None = None,
) -> dict:
    """Build AllPrintings-like dict for card_to_product compilation.

    Constructs a data structure with the same shape that
    :class:`MtgjsonCardLinker` (from mtg-sealed-content) expects, but
    sourced entirely from pipeline artefacts instead of AllPrintings.json.

    Args:
        contents_dict: From compile_contents().
            ``{set_code: {product_name: {contents…}}}``
        boosters_raw: Raw taw booster data.
            ``{SET_CODE: {booster_code: {sheets, boosters, …}}}``
        decks_raw: Raw taw deck list.
            ``[{name, set_code, cards, sideboard, …}]``
        card_finishes: From build_card_finishes_lookup().
            ``{mtgjson_uuid: {finishes, number, set}}``
        products_dict: From compile_products().
            ``{set_code: {product_name: {…}}}``
        uuid_map: From build_uuid_map_from_pipeline(), keyed by lowercase set
            code. Only ``cards_by_language``/``tokens_by_language`` are used
            here, to let a language-tagged sealed product's ``deck:``
            reference resolve to that language's card UUIDs instead of the
            deck's own (English-default) ones -- see
            ``_ctp_get_cards_in_deck``.

    Returns:
        ``{SET_CODE: {"sealedProduct": [...], "booster": {...}, "decks": [...],
                      "cards": [...], "tokens": [],
                      "cards_by_language": {...}, "tokens_by_language": {...}}}``
    """
    uuid_map = uuid_map or {}
    view: dict[str, dict] = {}

    def _ensure_set(code: str) -> dict:
        upper = code.upper()
        if upper not in view:
            lower_map = uuid_map.get(code.lower(), {})
            view[upper] = {
                "sealedProduct": [],
                "booster": {},
                "decks": [],
                "cards": [],
                "tokens": [],
                "cards_by_language": lower_map.get("cards_by_language", {}),
                "tokens_by_language": lower_map.get("tokens_by_language", {}),
            }
        return view[upper]

    # collect all set codes
    for code in contents_dict:
        _ensure_set(code)
    for code in boosters_raw:
        _ensure_set(code)
    for deck_entry in decks_raw:
        _ensure_set(deck_entry["set_code"])
    for info in card_finishes.values():
        _ensure_set(info["set"])
    for code in products_dict:
        _ensure_set(code)

    # Products only present in the contents YAML have no pin to look up, so they
    # keep the historical name-based UUID.
    product_uuid_map = resolve_sealed_uuids(products_dict)

    for set_code, products in contents_dict.items():
        upper = set_code.upper()
        for product_name, product_contents in products.items():
            puuid = product_uuid_map.get((upper, product_name))
            if puuid is None:
                # No products.yaml entry, so no pin either. The name-based UUID
                # can collide with one a rename carried over to another product,
                # so say so rather than emitting a silent duplicate.
                puuid = name_uuid(product_name)
                LOGGER.warning(
                    "Sealed contents %s '%s' has no products.yaml entry, using its name-based UUID",
                    upper,
                    product_name,
                )
            # A product's own products.yaml "language" (e.g. "Japanese") is
            # separate from its contents; carry it along so deck: references
            # resolved below can pick that language's cards instead of the
            # deck's own English-default ones. See _ctp_get_cards_in_deck.
            product_language = (products_dict.get(set_code, {}).get(product_name) or {}).get("language")
            view[upper]["sealedProduct"].append(
                {
                    "uuid": puuid,
                    "name": product_name,
                    "contents": product_contents,
                    "language": product_language,
                }
            )

    for set_code, booster_config in boosters_raw.items():
        upper = set_code.upper()
        _ensure_set(upper)
        view[upper]["booster"] = booster_config

    # Group taw decks by set code, mapping to AllPrintings deck format.
    def _map_card(c: dict) -> dict:
        return {
            "uuid": c["mtgjson_uuid"],
            "isFoil": c.get("foil", False),
            "isEtched": c.get("etched", False),
            # Carried through (not part of AllPrintings' own deck card shape)
            # so _ctp_get_cards_in_deck can look up this card's language-
            # specific UUID when the requesting product has one set.
            "_set": c.get("set_code", "").upper(),
            "_number": c.get("number"),
        }

    def _map_board(raw_list: list | None) -> list[dict]:
        if not raw_list:
            return []
        return [_map_card(c) for c in raw_list]

    decks_by_set: dict[str, list[dict]] = defaultdict(list)
    for deck_entry in decks_raw:
        upper = deck_entry["set_code"].upper()

        mapped_cards = _map_board(deck_entry.get("cards"))
        mapped_main = mapped_cards  # taw "cards" → both cards AND mainBoard
        mapped_side = _map_board(deck_entry.get("sideboard"))
        mapped_commander = _map_board(deck_entry.get("commander"))
        mapped_display_commander = _map_board(deck_entry.get("displayCommander"))
        mapped_tokens = _map_board(deck_entry.get("tokens"))
        mapped_planar = _map_board(deck_entry.get("planarDeck"))
        mapped_scheme = _map_board(deck_entry.get("schemeDeck"))

        source_set_codes = [sc.upper() for sc in deck_entry.get("sourceSetCodes", [upper])]

        mapped_deck: dict[str, Any] = {
            "name": deck_entry["name"],
            "cards": mapped_cards,
            "mainBoard": mapped_main,
            "sideBoard": mapped_side,
            "commander": mapped_commander,
            "displayCommander": mapped_display_commander,
            "tokens": mapped_tokens,
            "planarDeck": mapped_planar,
            "planes": mapped_planar,
            "schemeDeck": mapped_scheme,
            "schemes": mapped_scheme,
            "sourceSetCodes": source_set_codes,
        }
        decks_by_set[upper].append(mapped_deck)

    for upper, deck_list in decks_by_set.items():
        _ensure_set(upper)
        view[upper]["decks"] = deck_list

    # Group card_finishes by set code; place all in "cards", leave "tokens" empty.
    # The compiler looks up UUIDs via linear scan of cards (and cards+tokens for
    # decks), so having everything in "cards" is correct and sufficient.
    cards_by_set: dict[str, list[dict]] = defaultdict(list)
    for uuid_val, info in card_finishes.items():
        upper = info["set"].upper()
        cards_by_set[upper].append(
            {
                "uuid": uuid_val,
                "finishes": info["finishes"],
                "number": info["number"],
            }
        )

    for upper, cards_list in cards_by_set.items():
        _ensure_set(upper)
        view[upper]["cards"] = cards_list

    LOGGER.info("Built pipeline view for %d sets", len(view))
    return view


class _PipelineCardLinker(CatalogWalker):
    def missing_deck_source(self, code: str) -> None:
        LOGGER.debug("Note: %s was NOT found in pipeline view", code)

    def unknown_content(self, key: str) -> None:
        LOGGER.warning("Unknown content_key in card_to_products: %s", key)

    def product_language(self, product: dict) -> str | None:
        return product.get("language")

    def deck_card_uuid(self, card: dict, language: str | None, set_code: str, deck_name: str) -> str:
        # Finish validation uses the deck's default UUID. Only the resulting
        # membership UUID changes for a product requesting another language.
        uuid: str = card["uuid"]
        if language:
            card_set, number = card.get("_set"), card.get("_number")
            set_map = self.mtgjson_data.get(card_set, {}) if card_set else {}
            by_lang = set_map.get("cards_by_language", {}).get(number, {})
            entry = by_lang.get(language) or set_map.get("tokens_by_language", {}).get(number, {}).get(language)
            if entry:
                uuid = entry[0]
            else:
                LOGGER.warning(
                    "Deck %s:%s has no %s printing of %s:%s; using the default-language UUID instead",
                    set_code,
                    deck_name,
                    language,
                    card_set,
                    number,
                )
        return uuid


def _ctp_get_cards_in_pack(data: dict, set_code: str, booster_code: str) -> list[_CTPCard]:
    return _PipelineCardLinker(data).get_cards_in_pack(set_code, booster_code)


def _ctp_get_cards_in_deck(data: dict, set_code: str, deck_name: str, language: str | None = None) -> list[_CTPCard]:
    return _PipelineCardLinker(data).get_cards_in_deck(set_code, deck_name, language)


def _ctp_get_cards_in_sealed_product(data: dict, set_code: str, sealed_product_uuid: str | None) -> list[_CTPCard]:
    return _PipelineCardLinker(data).get_cards_in_sealed_product(set_code, sealed_product_uuid)


def _ctp_get_cards_in_content_type(
    data: dict, content_key: str, content: dict[str, Any], language: str | None = None
) -> list[_CTPCard]:
    return _PipelineCardLinker(data).get_cards_in_content_type(content_key, content, language)


def compile_card_to_products(pipeline_view: dict) -> dict[str, dict[str, list[str]]]:
    """Compile card-to-products mapping from pipeline view dict.

    Replicates: mtg-sealed-content/scripts/card_to_product_compiler.py

    For every sealed product in every set, walks the product's contents
    tree (cards, packs, decks, sealed, variable) and records which
    cards (with finish) appear in which products.

    Args:
        pipeline_view: AllPrintings-like dict from :func:`build_pipeline_view`.

    Returns:
        ``{card_uuid: {finish: sorted([product_uuid, …])}}``.
    """
    build_data: dict[_CTPCard, set[str]] = defaultdict(set)
    linker = _PipelineCardLinker(pipeline_view)

    for set_code, set_data in pipeline_view.items():
        if not set_data.get("sealedProduct"):
            continue

        LOGGER.debug("card_to_products: processing %s", set_code)
        for sealed_product in set_data["sealedProduct"]:
            cards_list = linker.get_cards_in_sealed_product(set_code, sealed_product.get("uuid"))
            for ctp_card in cards_list:
                build_data[ctp_card].add(sealed_product.get("uuid"))

    result = _ctp_results_to_json(build_data)
    LOGGER.info(
        "Compiled card_to_products: %d card UUIDs across %d sets",
        len(result),
        len(pipeline_view),
    )
    return result
