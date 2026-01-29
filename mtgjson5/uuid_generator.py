"""
NumPy-vectorized UUID generation for MTGJSON.

These functions process entire arrays at once, minimizing Python loop overhead
while still using the standard library's uuid5 for RFC 4122 compliance.
"""

import hashlib
import uuid

import numpy as np
import polars as pl

NAMESPACE_DNS_BYTES = uuid.NAMESPACE_DNS.bytes


def _uuid5_from_string(name: str) -> str:
    """
    Generate UUID5 string from name using DNS namespace.

    Inlined implementation avoiding uuid.uuid5() object overhead.
    """
    hash_bytes = hashlib.sha1(NAMESPACE_DNS_BYTES + name.encode("utf-8")).digest()
    return str(uuid.UUID(bytes=hash_bytes[:16], version=5))


def compute_v5_uuids_numpy(
    scryfall_ids: np.ndarray,
    sides: np.ndarray,
    cached_uuids: np.ndarray | None = None,
) -> np.ndarray:
    """
    Compute MTGJSON v5 UUIDs from scryfall_id + side.

    Args:
        scryfall_ids: Array of Scryfall UUID strings
        sides: Array of side values ("a", "b", etc.), None becomes "a"
        cached_uuids: Optional array of cached UUIDs (use if not None/empty)

    Returns:
        Array of UUID strings
    """
    n = len(scryfall_ids)
    results = np.empty(n, dtype=object)

    for i in range(n):
        # Check cache first
        if cached_uuids is not None and cached_uuids[i] is not None:
            results[i] = cached_uuids[i]
            continue

        scryfall_id = scryfall_ids[i]
        if scryfall_id is None:
            results[i] = None
            continue

        side = sides[i] if sides[i] is not None else "a"
        id_source = f"{scryfall_id}{side}"
        results[i] = _uuid5_from_string(id_source)

    return results


def compute_v5_uuids_polars(series: pl.Series, sides: pl.Series) -> pl.Series:
    """Polars-compatible wrapper for v5 UUID generation."""
    scryfall_ids = series.to_numpy()
    sides_arr = sides.to_numpy()
    results = compute_v5_uuids_numpy(scryfall_ids, sides_arr)
    return pl.Series(results, dtype=pl.String)


def compute_v4_uuids_numpy(
    scryfall_ids: np.ndarray,
    names: np.ndarray,
    face_names: np.ndarray,
    types: np.ndarray,  # List[str] per row
    colors: np.ndarray,  # List[str] per row
    powers: np.ndarray,
    toughnesses: np.ndarray,
    sides: np.ndarray,
    set_codes: np.ndarray,
) -> np.ndarray:
    """
    Compute MTGJSON v4 UUIDs (legacy format).

    Token formula: face_name + colors + power + toughness + side + set_code[1:] + scryfall_id
    Normal formula: "sf" + scryfall_id + face_name

    Args:
        scryfall_ids: Scryfall UUID strings
        names: Card names
        face_names: Face names (None for single-faced)
        types: List of types per card (to detect tokens)
        colors: List of colors per card
        powers: Power values (None if not creature)
        toughnesses: Toughness values (None if not creature)
        sides: Side values ("a", "b", etc.)
        set_codes: Set codes (e.g., "NEO")

    Returns:
        Array of v4 UUID strings
    """
    n = len(scryfall_ids)
    results = np.empty(n, dtype=object)

    token_types = {"Token", "Card"}

    for i in range(n):
        scryfall_id = scryfall_ids[i] or ""
        name = names[i] or ""
        face_name = face_names[i]
        card_name = face_name if face_name else name

        # Check if token
        card_types = types[i] if types[i] is not None else []
        is_token = bool(token_types.intersection(card_types))

        if is_token:
            # Token formula
            card_colors = colors[i] if colors[i] is not None else []
            colors_str = "".join(card_colors)
            power = powers[i] or ""
            toughness = toughnesses[i] or ""
            side = sides[i] or ""
            set_code = set_codes[i] or ""
            set_suffix = set_code[1:].upper() if len(set_code) > 1 else ""

            id_source = f"{card_name}{colors_str}{power}{toughness}{side}{set_suffix}{scryfall_id}"
        else:
            # Normal card formula
            id_source = f"sf{scryfall_id}{card_name}"

        results[i] = _uuid5_from_string(id_source)

    return results


def compute_sealed_product_uuids_numpy(names: np.ndarray) -> np.ndarray:
    """
    Compute UUIDs for sealed products.

    Formula: uuid5(name)
    """
    n = len(names)
    results = np.empty(n, dtype=object)

    for i in range(n):
        name = names[i]
        if name is None:
            results[i] = None
        else:
            results[i] = _uuid5_from_string(name)

    return results


def uuid5_batch(series: pl.Series) -> pl.Series:
    """
    Batch UUID5 generation for use with map_batches.

    Usage:
        df.with_columns(
            pl.concat_str([pl.col("id"), pl.col("side").fill_null("a")])
            .map_batches(uuid5_batch, return_dtype=pl.String)
            .alias("uuid")
        )
    """
    arr = series.to_numpy()
    n = len(arr)
    results = np.empty(n, dtype=object)

    for i in range(n):
        val = arr[i]
        if val is None:
            results[i] = None
        else:
            results[i] = _uuid5_from_string(val)

    return pl.Series(results, dtype=pl.String)


def compute_v4_uuid_from_struct(struct_series: pl.Series) -> pl.Series:
    """
    Compute v4 UUID from a struct containing all required fields.

    Usage:
        df.with_columns(
            pl.struct([
                "id", "name", "face_name", "types", "colors",
                "power", "toughness", "side", "set"
            ])
            .map_batches(compute_v4_uuid_from_struct, return_dtype=pl.String)
            .alias("mtgjson_v4_id")
        )
    """
    # Unnest struct to dict rows
    rows = struct_series.struct.unnest().to_dicts()

    n = len(rows)
    results = np.empty(n, dtype=object)
    token_types = {"Token", "Card"}

    for i, row in enumerate(rows):
        scryfall_id = row.get("id") or ""
        name = row.get("name") or ""
        face_name = row.get("face_name")
        card_name = face_name if face_name else name

        card_types = row.get("types") or []
        is_token = bool(token_types.intersection(card_types))

        if is_token:
            card_colors = row.get("colors") or []
            colors_str = "".join(card_colors)
            power = row.get("power") or ""
            toughness = row.get("toughness") or ""
            side = row.get("side") or ""
            set_code = row.get("set") or ""
            set_suffix = set_code[1:].upper() if len(set_code) > 1 else ""

            id_source = f"{card_name}{colors_str}{power}{toughness}{side}{set_suffix}{scryfall_id}"
        else:
            id_source = f"sf{scryfall_id}{card_name}"

        results[i] = _uuid5_from_string(id_source)

    return pl.Series(results, dtype=pl.String)


def url_hash_batch(
    series: pl.Series, *, _return_dtype: pl.DataType | None = None
) -> pl.Series:
    """
    Optimized batch SHA256 hash generation for purchase URLs.

    Uses list comprehension for reliable string output.
    The return_dtype parameter is accepted for Polars map_batches compatibility.
    """
    if series.is_empty():
        return pl.Series([], dtype=pl.String)

    def hash_single(val: str | None) -> str | None:
        if val is None:
            return None
        return hashlib.sha256(val.encode("utf-8")).hexdigest()[:16]

    results = [hash_single(v) for v in series.to_list()]
    return pl.Series(results, dtype=pl.String)
