"""Stable UUID assignment for sealed products.

Sealed product UUIDs are ``uuid5(NAMESPACE_DNS, productName)``, so renaming a
product in mtg-sealed-content mints a brand new UUID and breaks every reference
to it — ``sealedProduct[].uuid``, ``contents.sealed[].uuid``,
``card.sourceProducts`` and the hashed purchase URLs.

This module keeps a pin file recording the UUID each product was first
published with, plus the identifiers it carried at the time.  A product that
has been renamed is recognised by its identifiers and keeps its original UUID.
Products with no pin fall back to the historical name-based formula, so pinning
never changes an already-published UUID.
"""

from __future__ import annotations

import json
import logging
import uuid as uuid_lib
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mtgjson5 import constants

if TYPE_CHECKING:
    import polars as pl

LOGGER = logging.getLogger(__name__)

PIN_RESOURCE_NAME = "sealed_product_uuids.json"

# Pin file layout::
#
#     {"BRO": {"The Brothers' War Bundle": {"uuid": "...",
#                                           "identifiers": {"mcmId": "677799"}}}}
#
# Set codes are upper case; product names are verbatim mtg-sealed-content keys.
# An entry carried through a rename also records "originalName", the name the
# UUID was minted from, so the pin stays auditable against the uuid5 formula.
PinFile = dict[str, dict[str, dict[str, Any]]]

# Products keyed the way compile_products() returns them:
# {set_code: {product_name: {"identifiers": {...}, ...}}}
ProductsDict = Mapping[str, Mapping[str, Mapping[str, Any]]]

_pins_cache: PinFile | None = None


def name_uuid(product_name: str) -> str:
    """Historical sealed product UUID: uuid5 over the product name alone."""
    return str(uuid_lib.uuid5(uuid_lib.NAMESPACE_DNS, product_name))


def load_pins(path: Path | None = None, *, refresh: bool = False) -> PinFile:
    """Load the sealed UUID pin file, caching it for the process."""
    global _pins_cache  # pylint: disable=global-statement

    if path is None and _pins_cache is not None and not refresh:
        return _pins_cache

    pin_path = path or (constants.RESOURCE_PATH / PIN_RESOURCE_NAME)
    if not pin_path.exists():
        LOGGER.warning("Sealed UUID pin file not found: %s", pin_path)
        pins: PinFile = {}
    else:
        with pin_path.open("rb") as fp:
            pins = json.loads(fp.read())
        LOGGER.info(
            "Loaded %d sealed UUID pins across %d sets",
            sum(len(v) for v in pins.values()),
            len(pins),
        )

    if path is None:
        _pins_cache = pins
    return pins


def _identifiers(info: Mapping[str, Any] | None) -> dict[str, str]:
    """Extract non-empty identifiers from a product entry as strings."""
    # Malformed upstream YAML can hand us a string or a list where a product
    # mapping belongs; the provider skips those, so we must not blow up on them.
    if not info or not isinstance(info, Mapping):
        return {}
    raw = info.get("identifiers") or {}
    if not isinstance(raw, Mapping):
        return {}
    return {str(k): str(v) for k, v in raw.items() if v is not None and v != ""}


def _match_renames(
    unpinned: dict[str, dict[str, str]],
    vacated: dict[str, dict[str, Any]],
    set_code: str,
) -> dict[str, str]:
    """Pair renamed products with the pins they vacated.

    ``unpinned`` maps product name -> identifiers for products with no pin under
    their current name.  ``vacated`` maps product name -> pin entry for pins
    whose name no longer appears in the build.

    A pin is reused only when the pairing is unambiguous in both directions: the
    product matches exactly one vacated pin, and that pin is claimed by exactly
    one product.  Anything less certain falls through to a fresh UUID rather
    than risk handing one product's UUID to another.
    """
    if not unpinned or not vacated:
        return {}

    # (field, value) -> vacated pin names carrying it
    index: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pin_name, pin in vacated.items():
        for field, value in _identifiers(pin).items():
            index[(field, value)].add(pin_name)

    candidates: dict[str, set[str]] = {}
    claimants: dict[str, set[str]] = defaultdict(set)
    for name, identifiers in unpinned.items():
        matched: set[str] = set()
        for field, value in identifiers.items():
            matched |= index.get((field, value), set())
        if matched:
            candidates[name] = matched
            for pin_name in matched:
                claimants[pin_name].add(name)

    resolved: dict[str, str] = {}
    for name, matched in candidates.items():
        if len(matched) != 1:
            LOGGER.warning(
                "Sealed UUID pin: %s '%s' matches %d retired products (%s), minting a new UUID",
                set_code,
                name,
                len(matched),
                ", ".join(sorted(matched)),
            )
            continue
        pin_name = next(iter(matched))
        if len(claimants[pin_name]) != 1:
            LOGGER.warning(
                "Sealed UUID pin: retired %s '%s' is claimed by %d products (%s), minting new UUIDs",
                set_code,
                pin_name,
                len(claimants[pin_name]),
                ", ".join(sorted(claimants[pin_name])),
            )
            continue
        resolved[name] = str(vacated[pin_name]["uuid"])
        LOGGER.info(
            "Sealed UUID pin: %s '%s' renamed to '%s', keeping %s",
            set_code,
            pin_name,
            name,
            resolved[name],
        )

    return resolved


def resolve_sealed_uuids(
    products: ProductsDict,
    pins: PinFile | None = None,
) -> dict[tuple[str, str], str]:
    """Resolve a stable UUID for every sealed product.

    Args:
        products: ``{set_code: {product_name: product_info}}`` as returned by
            :func:`mtgjson5.pipeline.stages.sealed.compile_products`.  Set codes
            may be any case.
        pins: Pin file contents; loaded from resources when omitted.

    Returns:
        ``{(SET_CODE, product_name): uuid}`` with upper-case set codes.
    """
    if pins is None:
        pins = load_pins()

    resolved: dict[tuple[str, str], str] = {}
    renamed = 0

    for raw_code, set_products in products.items():
        code = raw_code.upper()
        set_pins = pins.get(code, {})
        present = set(set_products)

        unpinned: dict[str, dict[str, str]] = {}
        for name, info in set_products.items():
            pin = set_pins.get(name)
            if pin and pin.get("uuid"):
                resolved[(code, name)] = str(pin["uuid"])
            else:
                unpinned[name] = _identifiers(info)

        vacated = {name: pin for name, pin in set_pins.items() if name not in present and pin.get("uuid")}
        rename_map = _match_renames(unpinned, vacated, code)
        renamed += len(rename_map)

        for name in unpinned:
            resolved[(code, name)] = rename_map.get(name) or name_uuid(name)

    _warn_on_collisions(resolved)

    if renamed:
        LOGGER.info("Sealed UUID pin: carried %d UUIDs through product renames", renamed)

    return resolved


def _warn_on_collisions(resolved: Mapping[tuple[str, str], str]) -> None:
    """Log any UUID shared by more than one product."""
    owners: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key, product_uuid in resolved.items():
        owners[product_uuid].append(key)

    for product_uuid, keys in owners.items():
        if len(keys) > 1:
            LOGGER.error(
                "Sealed UUID %s is shared by %d products: %s",
                product_uuid,
                len(keys),
                ", ".join(f"{code}/{name}" for code, name in sorted(keys)),
            )


def build_pins(products: ProductsDict, pins: PinFile | None = None) -> PinFile:
    """Build an updated pin file covering every product in ``products``.

    Existing pins are carried forward untouched apart from the name and
    identifiers they are filed under, which are refreshed to match the current
    data.  Retired products keep their pins so a later re-add or a delayed
    rename can still find them.  A pin that no longer matches its own name
    records ``originalName``, the name whose uuid5 minted it.
    """
    if pins is None:
        pins = load_pins()

    resolved = resolve_sealed_uuids(products, pins)

    updated: PinFile = {code: dict(entries) for code, entries in pins.items()}

    for raw_code, set_products in products.items():
        code = raw_code.upper()
        set_pins = updated.setdefault(code, {})
        written: set[str] = set()
        for name, info in set_products.items():
            product_uuid = resolved[(code, name)]
            original_name: str | None = None
            # A rename files the UUID under its new name; drop the stale entry.
            for old_name, pin in list(set_pins.items()):
                if old_name == name or pin.get("uuid") != product_uuid:
                    continue
                if old_name in written:
                    # Two live products resolved to the same UUID — already
                    # logged as an error.  Leave the first one's pin alone
                    # instead of deleting the entry we just wrote for it.
                    continue
                original_name = str(pin.get("originalName") or old_name)
                del set_pins[old_name]
            current = set_pins.get(name)
            if original_name is None and current and current.get("uuid") == product_uuid:
                original_name = current.get("originalName")
            entry: dict[str, Any] = {"uuid": product_uuid}
            if original_name and original_name != name:
                entry["originalName"] = original_name
            identifiers = _identifiers(info)
            if identifiers:
                entry["identifiers"] = dict(sorted(identifiers.items()))
            set_pins[name] = entry
            written.add(name)

    return {code: dict(sorted(entries.items())) for code, entries in sorted(updated.items()) if entries}


# Neither a set code nor a product name can contain a unit separator, so it is
# safe to build a single lookup key out of the pair.
_KEY_SEP = "\x1f"


def sealed_uuid_expr(products_lf: pl.LazyFrame, pins: PinFile | None = None) -> pl.Expr:
    """Build an expression resolving each row's pinned sealed product UUID.

    A row with no pin resolves to null so the caller can fall back to the
    name-based formula.  This is deliberately an expression rather than a join:
    ``sealedProduct`` array order in the output is the row order of the frame
    this is applied to, and a join is free to reshuffle rows.

    Args:
        products_lf: Frame carrying ``setCode``, ``productName`` and (optionally)
            an ``identifiers`` struct.
        pins: Pin file contents; loaded from resources when omitted.

    Returns:
        String expression yielding the pinned UUID, or null where none applies.
    """
    import polars as pl

    columns = ["setCode", "productName"]
    has_identifiers = "identifiers" in products_lf.collect_schema().names()
    if has_identifiers:
        columns.append("identifiers")

    df = products_lf.select(columns).unique(subset=["setCode", "productName"]).collect()

    products: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in df.iter_rows(named=True):
        identifiers = row.get("identifiers") if has_identifiers else None
        products[row["setCode"]][row["productName"]] = {"identifiers": identifiers or {}}

    resolved = resolve_sealed_uuids(products, pins)
    if not resolved:
        return pl.lit(None, dtype=pl.String)

    lookup = {f"{code}{_KEY_SEP}{name}": product_uuid for (code, name), product_uuid in resolved.items()}
    key = pl.concat_str([pl.col("setCode"), pl.lit(_KEY_SEP), pl.col("productName")])
    return key.replace_strict(lookup, default=None, return_dtype=pl.String)


def dump_pins(pins: PinFile, path: Path) -> None:
    """Write a pin file with stable ordering so diffs stay readable."""
    path.write_text(json.dumps(pins, indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
