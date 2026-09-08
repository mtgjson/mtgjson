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

Carrying a UUID through a rename leaves the old name free, so a later product
under that name would mint the same UUID a second time.  Resolution ends by
pulling those newcomers off the pinned product's UUID, since two sealed
products sharing one UUID misattributes ``sourceProducts`` between them.
"""

from __future__ import annotations

import hashlib
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


def _usable_products(set_products: Any, set_code: str) -> dict[str, Mapping[str, Any]]:
    """Products with a well-formed mapping body, the way the frame path sees them.

    ``_build_sealed_products_records`` drops products whose YAML body is not a
    mapping, so the sealed products frame never carries them.  Resolution is
    context sensitive — which names are present decides which pins count as
    vacated — so the dict callers have to agree on the same product set or the
    two paths can hand one product two different UUIDs in a single build.
    """
    if not isinstance(set_products, Mapping):
        LOGGER.warning("Sealed UUID pin: %s has a malformed products body, skipping the set", set_code)
        return {}
    usable = {name: info for name, info in set_products.items() if isinstance(info, Mapping)}
    if len(usable) != len(set_products):
        LOGGER.warning(
            "Sealed UUID pin: %s has %d malformed product entries, skipping them",
            set_code,
            len(set_products) - len(usable),
        )
    return usable


def _conflicts(identifiers: Mapping[str, str], pin_identifiers: Mapping[str, str]) -> bool:
    """Whether two identifier sets disagree on a field they both carry."""
    return any(field in pin_identifiers and pin_identifiers[field] != value for field, value in identifiers.items())


def _warn_on_identifier_drift(
    set_code: str,
    name: str,
    identifiers: Mapping[str, str],
    pin_identifiers: Mapping[str, str],
) -> None:
    """Report a pinned product whose identifiers no longer match its pin.

    Resolution matches on the current name before it looks at identifiers, so
    two products swapping names resolve to each other's UUID with nothing else
    to show for it.  The pin holds the evidence; say so rather than quietly
    refiling the swapped identifiers on the next refresh.
    """
    changed = {
        field: (pin_identifiers[field], value)
        for field, value in identifiers.items()
        if field in pin_identifiers and pin_identifiers[field] != value
    }
    if changed:
        LOGGER.warning(
            "Sealed UUID pin: %s '%s' carries identifiers its pin disagrees with (%s); "
            "check it is the same product before refreshing the pin file",
            set_code,
            name,
            ", ".join(f"{field}: {was} -> {now}" for field, (was, now) in sorted(changed.items())),
        )


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
    one product.  One shared identifier is not enough on its own either —
    vendors reuse and mistype ids, and the shipped pins already carry duplicate
    values within a set — so a candidate that disagrees on any identifier the
    two both carry is dropped.  Anything less certain falls through to a fresh
    UUID rather than risk handing one product's UUID to another.
    """
    if not unpinned or not vacated:
        return {}

    # (field, value) -> vacated pin names carrying it
    index: dict[tuple[str, str], set[str]] = defaultdict(set)
    pin_identifiers: dict[str, dict[str, str]] = {}
    for pin_name, pin in vacated.items():
        pin_identifiers[pin_name] = _identifiers(pin)
        for field, value in pin_identifiers[pin_name].items():
            index[(field, value)].add(pin_name)

    candidates: dict[str, set[str]] = {}
    claimants: dict[str, set[str]] = defaultdict(set)
    for name, identifiers in unpinned.items():
        matched: set[str] = set()
        for field, value in identifiers.items():
            matched |= index.get((field, value), set())
        contradicted = {pin_name for pin_name in matched if _conflicts(identifiers, pin_identifiers[pin_name])}
        for pin_name in sorted(contradicted):
            LOGGER.info(
                "Sealed UUID pin: %s '%s' shares an identifier with retired '%s' but contradicts another, ignoring it",
                set_code,
                name,
                pin_name,
            )
        matched -= contradicted
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
    pinned_keys: set[tuple[str, str]] = set()
    renamed = 0

    for raw_code, set_products in products.items():
        code = raw_code.upper()
        set_pins = pins.get(code, {})
        usable = _usable_products(set_products, code)
        present = set(usable)

        unpinned: dict[str, dict[str, str]] = {}
        for name, info in usable.items():
            pin = set_pins.get(name)
            if pin and pin.get("uuid"):
                resolved[(code, name)] = str(pin["uuid"])
                pinned_keys.add((code, name))
                _warn_on_identifier_drift(code, name, _identifiers(info), _identifiers(pin))
            else:
                unpinned[name] = _identifiers(info)

        vacated = {name: pin for name, pin in set_pins.items() if name not in present and pin.get("uuid")}
        rename_map = _match_renames(unpinned, vacated, code)
        renamed += len(rename_map)

        for name in unpinned:
            carried = rename_map.get(name)
            if carried:
                resolved[(code, name)] = carried
                pinned_keys.add((code, name))
            else:
                resolved[(code, name)] = name_uuid(name)

    _split_collisions(resolved, pinned_keys)

    if renamed:
        LOGGER.info("Sealed UUID pin: carried %d UUIDs through product renames", renamed)

    return resolved


def qualified_name(set_code: str, product_name: str) -> str:
    """Mint source for a product whose plain name UUID is already taken."""
    return f"{set_code} {product_name}"


def _split_collisions(resolved: dict[tuple[str, str], str], pinned_keys: set[tuple[str, str]]) -> None:
    """Pull live products off a UUID another live product already owns.

    Once a pin follows a product through a rename, upstream re-adding a product
    under the vacated name — or another set reusing that name, since the name
    formula ignores set codes — hands both products the same UUID.  Two
    ``sealedProduct`` entries then share a UUID and ``sourceProducts`` starts
    attributing one product's cards to the other.

    The pinned product keeps the UUID, since that is the one already published
    against it, and the newcomer is re-minted from a set-qualified name.  A
    collision with no pin behind it is the pre-existing duplicate-name case, so
    it is only reported: re-minting there would move a UUID that is already out
    in the wild.
    """
    owners: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for key, product_uuid in resolved.items():
        owners[product_uuid].append(key)

    for product_uuid, keys in sorted(owners.items()):
        if len(keys) == 1:
            continue
        keys.sort()
        listing = ", ".join(f"{code}/{name}" for code, name in keys)
        pinned = [key for key in keys if key in pinned_keys]
        if len(pinned) != 1:
            LOGGER.error("Sealed UUID %s is shared by %d products: %s", product_uuid, len(keys), listing)
            continue
        LOGGER.error(
            "Sealed UUID %s is shared by %d products (%s); keeping it on the pinned %s/%s",
            product_uuid,
            len(keys),
            listing,
            *pinned[0],
        )
        for code, name in keys:
            if (code, name) == pinned[0]:
                continue
            resolved[(code, name)] = name_uuid(qualified_name(code, name))
            LOGGER.error("Sealed UUID pin: minted %s for %s/%s instead", resolved[(code, name)], code, name)


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
        usable = _usable_products(set_products, code)
        for name, info in usable.items():
            product_uuid = resolved[(code, name)]
            original_name: str | None = None
            # A rename files the UUID under its new name; drop the stale entry.
            for old_name, pin in list(set_pins.items()):
                if old_name == name or pin.get("uuid") != product_uuid:
                    continue
                if old_name in usable:
                    # The pin belongs to a product that is still in the build,
                    # so this is a collision rather than a rename.  Deleting it
                    # would strip a live product of its published UUID.
                    continue
                original_name = str(pin.get("originalName") or old_name)
                del set_pins[old_name]
            current = set_pins.get(name)
            if original_name is None and current and current.get("uuid") == product_uuid:
                original_name = current.get("originalName")
            if original_name is None and product_uuid == name_uuid(qualified_name(code, name)):
                # Re-minted off a collision; record what it was minted from so
                # the pin stays reproducible from the uuid5 formula.
                original_name = qualified_name(code, name)
            entry: dict[str, Any] = {"uuid": product_uuid}
            if original_name and original_name != name:
                entry["originalName"] = original_name
            identifiers = _identifiers(info)
            if identifiers:
                entry["identifiers"] = dict(sorted(identifiers.items()))
            set_pins[name] = entry

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
        products[row["setCode"].upper()][row["productName"]] = {"identifiers": identifiers or {}}

    resolved = resolve_sealed_uuids(products, pins)
    if not resolved:
        return pl.lit(None, dtype=pl.String)

    lookup = {f"{code}{_KEY_SEP}{name}": product_uuid for (code, name), product_uuid in resolved.items()}
    # resolve_sealed_uuids() keys on upper-case set codes, so the lookup has to
    # as well or a lower-case row silently falls through to the name formula.
    key = pl.concat_str([pl.col("setCode").str.to_uppercase(), pl.lit(_KEY_SEP), pl.col("productName")])
    return key.replace_strict(lookup, default=None, return_dtype=pl.String)


def serialize_pins(pins: PinFile) -> str:
    """Render a pin file with stable ordering so diffs stay readable."""
    return json.dumps(pins, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def dump_pins(pins: PinFile, path: Path) -> None:
    """Write a pin file to disk."""
    path.write_text(serialize_pins(pins), encoding="utf-8")


def pin_file_digest(path: Path | None = None) -> str:
    """SHA-256 of the pin file, for cache keys that must follow pin edits."""
    pin_path = path or (constants.RESOURCE_PATH / PIN_RESOURCE_NAME)
    if not pin_path.exists():
        return ""
    return hashlib.sha256(pin_path.read_bytes()).hexdigest()
