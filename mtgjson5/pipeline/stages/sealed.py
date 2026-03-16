"""Sealed content compilation — inline replacements for mtg-sealed-content outputs."""

from __future__ import annotations

import itertools as itr
import logging
from pathlib import Path

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

    # ── 1. Cards lookup ──────────────────────────────────────────────
    cards_df = cards_lf.select(
        pl.col("id").alias("scryfallId"),
        pl.col("set").alias("set_lower"),
        pl.col("collector_number").alias("number"),
        pl.col("name"),
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

    # ── 2. Booster lookup ────────────────────────────────────────────
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

    # ── 3. Decks lookup ──────────────────────────────────────────────
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

    # ── 4. Sealed product lookup ─────────────────────────────────────
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
