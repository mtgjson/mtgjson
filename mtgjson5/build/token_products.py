"""Token product UUIDs and normalized token product tables.

Token product data comes verbatim from mtg-sealed-content as a mapping of
token uuid -> list of product dicts. A physical token can be listed under
each of its faces and under several vendor product IDs, so MTGJSON derives
its own product ``uuid`` from what the token physically is (its faces and
their face attributes) rather than from any vendor ID.

Derivation (see :func:`token_product_hash_input`):

1. For each entry in ``tokenParts``, the part key is its ``uuid``. A part with
   no ``uuid`` uses the compact JSON array ``[faceId, faceName]`` instead
   (e.g. ``["9","Zombie"]``), so it is neither dropped nor merged with
   another uuid-less part.
2. Each part becomes ``[partKey, sorted(faceAttribute or [])]``.
3. Duplicate parts are removed and the remaining parts sorted ascending.
4. The list is serialized as compact JSON (``separators=(",", ":")``,
   non-ASCII left unescaped, same as ``JSON.stringify``).
5. ``uuid = uuid5(NAMESPACE_DNS, thatString)``, using the same uuid5 helper
   as card uuids.

Listings that differ only by ``tcgplayerProductId`` share a uuid.
"""

from __future__ import annotations

import json
from typing import Any

import polars as pl

from mtgjson5.pipeline.expressions import uuid5_expr


def _part_key(part: dict[str, Any]) -> str:
    """Key for one token part: its uuid, or ``[faceId, faceName]`` as JSON."""
    uuid = part.get("uuid")
    if uuid:
        return str(uuid)
    return json.dumps([part.get("faceId"), part.get("faceName")], separators=(",", ":"), ensure_ascii=False)


def token_product_hash_input(product: dict[str, Any]) -> str:
    """Canonical string hashed to produce a token product's uuid."""
    parts = {
        (_part_key(part), tuple(sorted(part.get("faceAttribute") or [])))
        for part in product.get("tokenParts") or []
        if isinstance(part, dict)
    }
    canonical = [[key, list(attrs)] for key, attrs in sorted(parts)]
    return json.dumps(canonical, separators=(",", ":"), ensure_ascii=False)


def token_product_uuids(hash_inputs: list[str]) -> list[str]:
    """uuid5 (DNS namespace) of each hash input, via the repo's polars uuid5 helper."""
    if not hash_inputs:
        return []
    return pl.DataFrame({"_key": hash_inputs}, schema={"_key": pl.String}).select(uuid5_expr("_key"))["_key"].to_list()


def add_token_product_uuids(token_products: dict[str, list[dict[str, Any]]]) -> None:
    """Set ``uuid`` on every product dict in ``token_products``, in place."""
    products = [p for plist in token_products.values() for p in plist if isinstance(p, dict)]
    uuids = token_product_uuids([token_product_hash_input(p) for p in products])
    for product, uuid in zip(products, uuids, strict=True):
        product["uuid"] = uuid


def build_token_product_tables(token_products: dict[str, list[dict[str, Any]]] | None) -> dict[str, pl.DataFrame]:
    """Build the ``tokenProducts``, ``tokenProductParts`` and ``tokenProductIdentifiers`` tables.

    Expects products that already carry ``uuid`` (see :func:`add_token_product_uuids`).
    Rows are deduped and sorted so output is reproducible.
    """
    if not token_products:
        return {}

    products: dict[str, list[str]] = {}
    parts: set[tuple[str, str]] = set()
    identifiers: dict[tuple[str, str], str | None] = {}

    for plist in token_products.values():
        for product in plist:
            product_uuid = product.get("uuid") if isinstance(product, dict) else None
            if not product_uuid:
                continue
            token_parts = [p for p in product.get("tokenParts") or [] if isinstance(p, dict)]
            if product_uuid not in products:
                products[product_uuid] = sorted({a for p in token_parts for a in p.get("faceAttribute") or []})
            for part in token_parts:
                if part.get("uuid"):
                    parts.add((product_uuid, part["uuid"]))
            product_id = (product.get("identifiers") or {}).get("tcgplayerProductId")
            if product_id:
                key = (product_uuid, str(product_id))
                url = (product.get("purchaseUrls") or {}).get("tcgplayer")
                if identifiers.get(key) is None:
                    identifiers[key] = url

    tables: dict[str, pl.DataFrame] = {}
    if products:
        tables["tokenProducts"] = pl.DataFrame(
            {
                "uuid": list(products),
                "faceAttribute": [attrs or None for attrs in products.values()],
            },
            schema={"uuid": pl.String, "faceAttribute": pl.List(pl.String)},
        ).sort("uuid")
    if parts:
        tables["tokenProductParts"] = pl.DataFrame(
            sorted(parts),
            schema={"tokenProductUuid": pl.String, "uuid": pl.String},
            orient="row",
        )
    if identifiers:
        tables["tokenProductIdentifiers"] = pl.DataFrame(
            [(pu, pid, url) for (pu, pid), url in identifiers.items()],
            schema={"tokenProductUuid": pl.String, "tcgplayerProductId": pl.String, "tcgplayerPurchaseUrl": pl.String},
            orient="row",
        ).sort("tokenProductUuid", "tcgplayerProductId")
    return tables
