"""Sealed content compilation — inline replacements for mtg-sealed-content outputs."""

from __future__ import annotations

import itertools as itr
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

import ijson
import polars as pl
import yaml

from mtgjson5.pipeline.stages.explode import _uuid5_concat_expr, _uuid5_expr

LOGGER = logging.getLogger(__name__)


def compile_products(products_dir: Path) -> dict:
    """Compile products.json from YAML source files.

    Replicates: mtg-sealed-content/scripts/new_products_compiler.py

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
        LOGGER.debug("Loaded %d products for %s from %s", len(products), code, file.name)
        result[code] = products
    LOGGER.info("Compiled products for %d sets", len(result))
    return result


class card:
    def __init__(self, contents):
        self.name = contents["name"]
        self.set = contents["set"]
        self.number = contents["number"]
        self.etched = contents.get("etched", False)
        self.foil = contents.get("foil", False)
        self.uuid = contents.get("uuid", False)

    def toJson(self):
        data = {"name": self.name, "set": self.set, "number": str(self.number)}
        if self.uuid:
            data["uuid"] = self.uuid
        if self.foil:
            data["foil"] = self.foil
        if self.etched:
            data["etched"] = self.etched
        return data

    def get_uuids(self, uuid_map):
        try:
            self.uuid = uuid_map[self.set.lower()]["cards"][str(self.number)][0]
            if self.name not in uuid_map[self.set.lower()]["cards"][str(self.number)][1]:
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


class pack:
    def __init__(self, contents):
        self.set = contents["set"]
        self.code = contents["code"]

    def toJson(self):
        data = {"set": self.set, "code": self.code}
        return data

    def get_uuids(self, uuid_map):
        try:
            umap = uuid_map[self.set.lower()]["booster"]
        except KeyError:
            umap = False
        if not umap or (self.code not in umap):
            LOGGER.warning("Booster code %s not found in set %s", self.code, self.set)


class deck:
    def __init__(self, contents):
        self.set = contents["set"]
        self.name = contents["name"]

    def toJson(self):
        data = {"set": self.set, "name": self.name}
        return data

    def get_uuids(self, uuid_map):
        try:
            umap = uuid_map[self.set.lower()]["decks"]
        except KeyError:
            umap = False
        if not umap or (self.name not in umap):
            LOGGER.warning("Deck named %s not found in set %s", self.name, self.set)


class sealed:
    def __init__(self, contents):
        self.set = contents["set"]
        self.count = contents["count"]
        self.name = contents["name"]
        self.uuid = contents.get("uuid", False)

    def toJson(self):
        data = {"set": self.set, "count": self.count, "name": self.name}
        if self.uuid:
            data["uuid"] = self.uuid
        return data

    def get_uuids(self, uuid_map):
        try:
            self.uuid = uuid_map[self.set.lower()]["sealedProduct"][self.name]
        except KeyError:
            LOGGER.warning("Product name %s not found in set %s", self.name, self.set)
            self.uuid = None


class other:
    def __init__(self, contents):
        self.name = contents["name"]

    def toJson(self):
        data = {"name": self.name}
        return data


class product:
    def __init__(self, contents, set_code=None, name=None):
        self.name = name
        self.set_code = set_code
        if not contents:
            contents = {}
        self.card = []
        for c in contents.get("card", []):
            self.card.append(card(c))
        self.pack = []
        for p in contents.get("pack", []):
            self.pack.append(pack(p))
        self.deck = []
        for d in contents.get("deck", []):
            self.deck.append(deck(d))
        self.sealed = []
        for s in contents.get("sealed", []):
            if s["name"] == self.name:
                raise ValueError(f"Self-referrential product {self.name}")
            self.sealed.append(sealed(s))
        self.other = []
        for o in contents.get("other", []):
            self.other.append(other(o))
            if o["name"] == "Bonus card unknown":
                LOGGER.warning("Product name %s missing bonus card definition", self.name)
        self.chance = contents.get("chance", 1)
        self.weight = contents.get("weight", 0)

        self.card_count = contents.get("card_count", 0)

        self.variable = []
        if "variable_mode" in contents:
            options = contents.pop("variable_mode")
            if options.get("replacement", False):
                for combo in itr.combinations_with_replacement(contents["variable"], options.get("count", 1)):
                    p_temp = product({})
                    for c in combo:
                        p_temp.merge(product(c))
                    self.variable.append(p_temp)
            else:
                for combo in itr.combinations(contents["variable"], options.get("count", 1)):
                    p_temp = product({})
                    for c in combo:
                        p_temp.merge(product(c))
                    self.variable.append(p_temp)
            if "weight" in options:
                if sum(v.chance for v in self.variable) != options["weight"]:
                    raise ValueError(f"Weight incorrectly assigned for product {self.name}")
            else:
                options["weight"] = sum(v.chance for v in self.variable)
            for v in self.variable:
                v.weight = options["weight"]
        elif "variable" in contents:
            self.variable = [product(p) for p in contents["variable"]]

    def merge(self, target):
        self.card += target.card
        self.pack += target.pack
        self.deck += target.deck
        self.sealed += target.sealed
        self.variable += target.variable
        self.card_count += target.card_count
        self.other += target.other
        self.chance *= target.chance

    def toJson(self):
        data = {}
        if self.card:
            data["card"] = [c.toJson() for c in self.card]
        if self.pack:
            data["pack"] = [p.toJson() for p in self.pack]
        if self.deck:
            data["deck"] = [d.toJson() for d in self.deck]
        if self.sealed:
            data["sealed"] = [s.toJson() for s in self.sealed]
        if self.other:
            data["other"] = [o.toJson() for o in self.other]
        if self.variable:
            data["variable"] = [{"configs": [v.toJson() for v in self.variable]}]
        if self.card_count:
            data["card_count"] = self.card_count
        if self.weight:
            data["variable_config"] = [{"chance": self.chance, "weight": self.weight}]
        return data

    def get_uuids(self, uuid_map):
        if self.name:
            try:
                self.uuid = uuid_map[self.set_code.lower()]["sealedProduct"][self.name]
            except KeyError:
                LOGGER.warning("Product name %s not found in set %s", self.name, self.set_code)
                self.uuid = None
        else:
            self.uuid = None
        for c in self.card:
            c.get_uuids(uuid_map)
        for p in self.pack:
            p.get_uuids(uuid_map)
        for d in self.deck:
            d.get_uuids(uuid_map)
        for s in self.sealed:
            s.get_uuids(uuid_map)
        for v in self.variable:
            v.get_uuids(uuid_map)


def build_uuid_map(allprintings_path: Path) -> dict:
    """Parse AllPrintings.json via streaming to build a UUID lookup map.

    Returns a dict keyed by lowercase set code with sub-keys:
        booster: set of booster type codes
        decks: set of deck names
        sealedProduct: {product_name: uuid}
        cards: {number_str: (uuid, name)}  — only side "a" cards
    """
    LOGGER.info("Loading AllPrintings.json from %s ...", allprintings_path)
    uuids: dict = {}
    current_set = ""
    ccode = ""
    status = ""
    name = ""
    number = ""
    uuid = ""
    holding = ""

    with open(allprintings_path, "rb") as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix == "data" and event == "map_key":
                current_set = value
                ccode = current_set.lower()
                uuids[ccode] = {
                    "booster": set(),
                    "decks": set(),
                    "sealedProduct": {},
                    "cards": {},
                }
                status = ""
            elif prefix == f"data.{current_set}" and event == "map_key":
                status = value
            elif status == "booster" and prefix == f"data.{current_set}.booster" and event == "map_key":
                uuids[ccode]["booster"].add(value)
            elif status == "decks" and prefix == f"data.{current_set}.decks.item.name":
                uuids[ccode]["decks"].add(value)
            elif status == "sealedProduct":
                if prefix == f"data.{current_set}.sealedProduct.item" and event == "start_map":
                    name = ""
                    uuid = ""
                elif prefix == f"data.{current_set}.sealedProduct.item.name":
                    name = value
                elif prefix == f"data.{current_set}.sealedProduct.item.uuid":
                    uuid = value
                elif prefix == f"data.{current_set}.sealedProduct.item" and event == "end_map":
                    uuids[ccode]["sealedProduct"][name] = uuid
            elif status == "cards":
                if prefix == f"data.{current_set}.cards.item.side" and value != "a":
                    holding = "skip"
                if prefix == f"data.{current_set}.cards.item" and event == "start_map":
                    number = ""
                    name = ""
                    uuid = ""
                elif prefix == f"data.{current_set}.cards.item.number":
                    number = value
                elif prefix == f"data.{current_set}.cards.item.name":
                    name = value
                elif prefix == f"data.{current_set}.cards.item.uuid":
                    uuid = value
                elif prefix == f"data.{current_set}.cards.item" and event == "end_map":
                    if holding != "skip":
                        uuids[ccode]["cards"][number] = (uuid, name)
                    holding = ""

    LOGGER.info("Built UUID map for %d sets", len(uuids))
    return uuids


def build_uuid_map_from_pipeline(
    cards_lf: pl.LazyFrame,
    uuid_cache_lf: pl.LazyFrame | None,
    boosters_raw: dict,
    decks_raw: list,
    products_dict: dict,
) -> dict:
    """Build the same UUID lookup map as build_uuid_map(), but from pipeline LazyFrames.

    This eliminates the AllPrintings.json dependency for sealed compilation.

    Returns a dict keyed by lowercase set code with sub-keys:
        cards: {number_str: (uuid, name)}  — only side "a" cards
        booster: set of booster type codes
        decks: set of deck names
        sealedProduct: {product_name: uuid}
    """
    uuids: dict = {}

    # all_cards.ndjson includes all languages — multiple entries per
    # (set, collector_number) with different Scryfall IDs. We need
    # exactly one entry per (set, collector_number), preferring English
    # but falling back to any language for cards that only exist in
    # non-English variants (e.g. Phyrexian-language cards).
    cards_df = (
        cards_lf.with_columns(pl.when(pl.col("lang") == "en").then(0).otherwise(1).alias("_lang_rank"))
        .sort("set", "collector_number", "_lang_rank")
        .unique(subset=["set", "collector_number"], keep="first")
        .select(
            pl.col("id").alias("scryfallId"),
            pl.col("set").alias("set_lower"),
            pl.col("collector_number").alias("number"),
            pl.col("name"),
        )
    )

    if uuid_cache_lf is not None:
        # Filter cache to side "a" only, then left-join
        cache_a = uuid_cache_lf.filter(pl.col("side") == "a").select("scryfallId", "cachedUuid")
        cards_df = cards_df.join(cache_a, on="scryfallId", how="left")
    else:
        cards_df = cards_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("cachedUuid"))

    # Add a literal side column for uuid5_concat (always "a" since we only want
    # front-face cards for the sealed UUID map).
    cards_df = cards_df.with_columns(pl.lit("a").alias("side"))

    # Compute MTGJSON UUID: coalesce(cachedUuid, uuid5(scryfallId || "a"))
    cards_df = cards_df.with_columns(
        pl.coalesce(
            pl.col("cachedUuid"),
            _uuid5_concat_expr(pl.col("scryfallId"), pl.col("side"), default="a"),
        ).alias("uuid")
    )

    cards_collected = cards_df.collect()

    for row in cards_collected.iter_rows(named=True):
        set_lower = row["set_lower"]
        if set_lower not in uuids:
            uuids[set_lower] = {
                "cards": {},
                "booster": set(),
                "decks": set(),
                "sealedProduct": {},
            }
        uuids[set_lower]["cards"][row["number"]] = (row["uuid"], row["name"])

    for set_code, booster_config in boosters_raw.items():
        code = set_code.lower()
        if code not in uuids:
            uuids[code] = {
                "cards": {},
                "booster": set(),
                "decks": set(),
                "sealedProduct": {},
            }
        uuids[code]["booster"] = set(booster_config.keys())

    for deck_entry in decks_raw:
        code = deck_entry["set_code"].lower()
        if code not in uuids:
            uuids[code] = {
                "cards": {},
                "booster": set(),
                "decks": set(),
                "sealedProduct": {},
            }
        uuids[code]["decks"].add(deck_entry["name"])


    # Gather all (set_code, product_name) pairs
    product_pairs: list[tuple[str, str]] = []
    for set_code, products in products_dict.items():
        for product_name in products:
            product_pairs.append((set_code.lower(), product_name))

    if product_pairs:
        product_names = [p[1] for p in product_pairs]
        product_df = pl.DataFrame({"productName": product_names})
        product_df = product_df.with_columns(_uuid5_expr("productName").alias("uuid"))
        product_uuids = product_df["uuid"].to_list()

        for (set_lower, product_name), puuid in zip(product_pairs, product_uuids, strict=True):
            if set_lower not in uuids:
                uuids[set_lower] = {
                    "cards": {},
                    "booster": set(),
                    "decks": set(),
                    "sealedProduct": {},
                }
            uuids[set_lower]["sealedProduct"][product_name] = puuid

    LOGGER.info("Built UUID map from pipeline for %d sets", len(uuids))
    return uuids


def build_card_finishes_lookup(
    cards_lf: pl.LazyFrame,
    uuid_cache_lf: pl.LazyFrame | None,
) -> dict[str, dict]:
    """Build {mtgjson_uuid: {"finishes": [...], "number": str, "set": str}} from Scryfall.

    Used by card_to_products compilation to determine card finish types.
    Includes both regular cards and tokens (layout == "token").

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


def compile_contents(contents_dir: Path, uuid_map: dict) -> tuple[dict, dict]:
    """Compile contents.json and deck_map.json from YAML source files.

    Replicates: mtg-sealed-content/scripts/product_contents_compiler.py

    Args:
        contents_dir: Path to directory containing per-set content YAML files.
        uuid_map: UUID lookup map from build_uuid_map().

    Returns:
        Tuple of (contents_dict, deck_map_dict).
    """
    products_contents: dict = {}

    for set_file in sorted(contents_dir.glob("*.yaml")):
        contents = yaml.safe_load(set_file.read_bytes())
        code = contents["code"]
        products_contents[code] = {}

        for name, p in contents["products"].items():
            if not p:
                LOGGER.warning("Product %s - %s missing contents", code, name)
                continue
            if set(p.keys()) == {"copy"}:
                p = contents["products"][p["copy"]]
            compiled_product = product(p, code, name)
            compiled_product.get_uuids(uuid_map)
            products_contents[code][name] = compiled_product

        if not products_contents[code]:
            products_contents.pop(code)

    LOGGER.info("Compiled contents for %d sets", len(products_contents))

    contents_dict = {k: set_to_json(v) for k, v in products_contents.items()}
    deck_map_dict = deck_links(products_contents)
    return contents_dict, deck_map_dict


def deck_links(all_products: dict) -> dict:
    """Build a mapping of deck set/name to sealed product UUIDs that contain them."""
    deck_mapper: dict = {}
    for set_contents in all_products.values():
        for product_contents in set_contents.values():
            if not product_contents.uuid:
                continue
            for d in product_contents.deck:
                if d.set not in deck_mapper:
                    deck_mapper[d.set] = {}
                if d.name not in deck_mapper[d.set]:
                    deck_mapper[d.set][d.name] = []
                deck_mapper[d.set][d.name].append(product_contents.uuid)
    return deck_mapper

def build_pipeline_view(
    contents_dict: dict,
    boosters_raw: dict,
    decks_raw: list,
    card_finishes: dict,
    products_dict: dict,
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

    Returns:
        ``{SET_CODE: {"sealedProduct": [...], "booster": {...}, "decks": [...],
                      "cards": [...], "tokens": []}}``
    """
    view: dict[str, dict] = {}

    def _ensure_set(code: str) -> dict:
        upper = code.upper()
        if upper not in view:
            view[upper] = {
                "sealedProduct": [],
                "booster": {},
                "decks": [],
                "cards": [],
                "tokens": [],
            }
        return view[upper]

    # collect all set codes
    for code in contents_dict:
        _ensure_set(code)
    for code in boosters_raw:
        _ensure_set(code)
    for deck_entry in decks_raw:
        _ensure_set(deck_entry["set_code"])
    for _uuid_val, info in card_finishes.items():
        _ensure_set(info["set"])
    for code in products_dict:
        _ensure_set(code)

    # Batch-compute sealedProduct UUIDs.
    all_product_pairs: list[tuple[str, str]] = []
    for set_code in contents_dict:
        for product_name in contents_dict[set_code]:
            all_product_pairs.append((set_code.upper(), product_name))

    product_uuid_map: dict[tuple[str, str], str] = {}
    if all_product_pairs:
        names = [p[1] for p in all_product_pairs]
        pdf = pl.DataFrame({"productName": names})
        pdf = pdf.with_columns(_uuid5_expr("productName").alias("uuid"))
        uuids_list = pdf["uuid"].to_list()
        product_uuid_map = {
            pair: puuid
            for pair, puuid in zip(all_product_pairs, uuids_list, strict=True)
        }

    for set_code, products in contents_dict.items():
        upper = set_code.upper()
        for product_name, product_contents in products.items():
            puuid = product_uuid_map.get((upper, product_name))
            view[upper]["sealedProduct"].append(
                {
                    "uuid": puuid,
                    "name": product_name,
                    "contents": product_contents,
                }
            )

    for set_code, booster_config in boosters_raw.items():
        upper = set_code.upper()
        _ensure_set(upper)
        view[upper]["booster"] = booster_config

    # Group taw decks by set code, mapping to AllPrintings deck format.
    decks_by_set: dict[str, list[dict]] = defaultdict(list)
    for deck_entry in decks_raw:
        upper = deck_entry["set_code"].upper()

        def _map_card(c: dict) -> dict:
            return {
                "uuid": c["mtgjson_uuid"],
                "isFoil": c.get("foil", False),
                "isEtched": c.get("etched", False),
            }

        def _map_board(raw_list: list | None) -> list[dict]:
            if not raw_list:
                return []
            return [_map_card(c) for c in raw_list]

        mapped_cards = _map_board(deck_entry.get("cards"))
        mapped_main = mapped_cards  # taw "cards" → both cards AND mainBoard
        mapped_side = _map_board(deck_entry.get("sideboard"))
        mapped_commander = _map_board(deck_entry.get("commander"))
        mapped_display_commander = _map_board(deck_entry.get("displayCommander"))
        mapped_tokens = _map_board(deck_entry.get("tokens"))
        mapped_planar = _map_board(deck_entry.get("planarDeck"))
        mapped_scheme = _map_board(deck_entry.get("schemeDeck"))

        source_set_codes = [
            sc.upper() for sc in deck_entry.get("sourceSetCodes", [upper])
        ]

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


class _CTPCard:
    """Card with finish for card-to-products mapping."""

    __slots__ = ("finish", "uuid")

    def __init__(self, uuid: str, finish: str) -> None:
        self.uuid = uuid
        self.finish = finish

    def __hash__(self) -> int:
        return hash((self.uuid, self.finish))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _CTPCard):
            return False
        return self.uuid == other.uuid and self.finish == other.finish


def _ctp_get_card_obj_from_card(card_content: dict[str, Any]) -> list[_CTPCard]:
    """Extract a :class:`_CTPCard` from a card content dict.

    Finish is ``"foil"`` if the foil flag is set, otherwise ``"nonfoil"``.
    """
    finish = "foil" if card_content.get("foil") else "nonfoil"
    if "uuid" in card_content:
        return [_CTPCard(card_content["uuid"], finish)]
    return []


def _ctp_get_cards_in_pack(
    data: dict, set_code: str, booster_code: str
) -> list[_CTPCard]:
    """Return cards reachable from a booster pack definition.

    Traverses every sheet referenced by boosters for *booster_code* and resolves
    finishes using the same special-case logic as the reference compiler.
    """
    try:
        booster_data = data[set_code].get("booster")
    except KeyError:
        return []
    if not booster_data:
        return []

    sheet_data = booster_data.get(booster_code)
    if not sheet_data:
        return []

    sheets_to_poll: set[str] = set()
    for booster in sheet_data["boosters"]:
        sheets_to_poll.update(booster["contents"].keys())

    return_value: set[_CTPCard] = set()
    for sheet in sheets_to_poll:
        cards_in_sheet = sheet_data["sheets"][sheet]["cards"]

        for card_uuid in cards_in_sheet:
            finish = "nonfoil"
            code = ""

            if sheet_data["sheets"][sheet]["foil"]:
                finishes: list[str] = []
                for source_code in sheet_data["sourceSetCodes"]:
                    if source_code not in data:
                        continue
                    for c in data[source_code]["cards"]:
                        if card_uuid == c["uuid"]:
                            finishes = c["finishes"]
                            code = source_code

                            # MH2 special case: only numbers 262-441 get etched treatment
                            if code == "MH2":
                                try:
                                    num = int(c["number"])
                                except (ValueError, TypeError):
                                    code = ""
                                else:
                                    if num < 262 or num > 441:
                                        code = ""

                # "etched" in sheet name or single finish "etched" → etched
                if ("etched" in sheet.lower() or len(finishes) == 1) and "etched" in finishes:
                    finish = "etched"
                elif "foil" in finishes:
                    finish = "foil"

            return_value.add(_CTPCard(card_uuid, finish))

            # Upstream does not track etched version of these cards — duplicate
            if code and code in ("H1R", "MH2", "STA"):
                return_value.add(_CTPCard(card_uuid, "etched"))

    return list(return_value)


def _ctp_get_cards_in_deck(
    data: dict, set_code: str, deck_name: str
) -> list[_CTPCard]:
    """Return cards from a named deck, validating finishes against source sets."""
    try:
        decks_data = data[set_code].get("decks")
    except KeyError:
        return []
    if not decks_data:
        return []

    return_value: set[_CTPCard] = set()
    for d in decks_data:
        if d["name"] != deck_name:
            continue

        deck_cards = (
            d.get("cards", [])
            + d.get("mainBoard", [])
            + d.get("sideBoard", [])
            + d.get("displayCommander", [])
            + d.get("commander", [])
            + d.get("tokens", [])
            + d.get("schemes", [])
            + d.get("planes", [])
            + d.get("planarDeck", [])
            + d.get("schemeDeck", [])
        )

        for deck_card in deck_cards:
            finish = "nonfoil"
            finishes: list[str] = []
            for code in d.get("sourceSetCodes", []):
                if code not in data:
                    LOGGER.debug("Note: %s was NOT found in pipeline view", code)
                    continue
                for c in data[code]["cards"] + data[code]["tokens"]:
                    if deck_card["uuid"] == c["uuid"]:
                        finishes = c["finishes"]
                        break

            # isEtched takes precedence over isFoil
            if deck_card.get("isEtched", False) and "etched" in finishes:
                finish = "etched"
            elif deck_card.get("isFoil", False) and "foil" in finishes:
                finish = "foil"

            return_value.add(_CTPCard(deck_card["uuid"], finish))
        break

    return list(return_value)


def _ctp_get_cards_in_sealed_product(
    data: dict, set_code: str, sealed_product_uuid: str | None
) -> list[_CTPCard]:
    """Return all cards reachable from a sealed product, traversing contents."""
    return_value: set[_CTPCard] = set()

    if set_code not in data:
        return []

    for sealed_product in data[set_code].get("sealedProduct", []):
        if sealed_product_uuid != sealed_product.get("uuid"):
            continue

        for content_key, contents in sealed_product.get("contents", {}).items():
            for content in contents:
                cards = _ctp_get_cards_in_content_type(data, content_key, content)
                return_value.update(cards)
        break

    return list(return_value)


def _ctp_get_cards_in_content_type(
    data: dict, content_key: str, content: dict[str, Any]
) -> list[_CTPCard]:
    """Dispatch to the appropriate handler for a content type."""
    if content_key == "card":
        return _ctp_get_card_obj_from_card(content)

    if content_key == "pack":
        return _ctp_get_cards_in_pack(data, content["set"].upper(), content["code"])

    if content_key == "sealed":
        return _ctp_get_cards_in_sealed_product(
            data, content["set"].upper(), content.get("uuid")
        )

    if content_key == "deck":
        return _ctp_get_cards_in_deck(data, content["set"].upper(), content["name"])

    if content_key == "variable":
        result: set[_CTPCard] = set()
        for config in content["configs"]:
            for dk in config.get("deck", []):
                result.update(
                    _ctp_get_cards_in_deck(data, dk["set"].upper(), dk["name"])
                )
            for sl in config.get("sealed", []):
                result.update(
                    _ctp_get_cards_in_sealed_product(
                        data, sl["set"].upper(), sl.get("uuid")
                    )
                )
            for pk in config.get("pack", []):
                result.update(
                    _ctp_get_cards_in_pack(data, pk["set"].upper(), pk["code"])
                )
            for cd in config.get("card", []):
                result.update(_ctp_get_card_obj_from_card(cd))
        return list(result)

    if content_key == "other":
        return []

    LOGGER.warning("Unknown content_key in card_to_products: %s", content_key)
    return []


def _ctp_results_to_json(
    build_data: dict[_CTPCard, set[str]],
) -> dict[str, dict[str, list[str]]]:
    """Convert build_data mapping to JSON-serializable dict."""
    return_value: dict[str, dict[str, list[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for ctp_card, product_uuids in build_data.items():
        return_value[ctp_card.uuid][ctp_card.finish] = sorted(product_uuids)
    return dict(return_value)


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

    for set_code, set_data in pipeline_view.items():
        if not set_data.get("sealedProduct"):
            continue

        LOGGER.debug("card_to_products: processing %s", set_code)
        for sealed_product in set_data["sealedProduct"]:
            cards_list = _ctp_get_cards_in_sealed_product(
                pipeline_view, set_code, sealed_product.get("uuid")
            )
            for ctp_card in cards_list:
                build_data[ctp_card].add(sealed_product.get("uuid"))

    result = _ctp_results_to_json(build_data)
    LOGGER.info(
        "Compiled card_to_products: %d card UUIDs across %d sets",
        len(result),
        len(pipeline_view),
    )
    return result
