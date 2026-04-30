"""
CardSphere provider - downloads set CSVs and builds ID mappings.

Fetches the set index from cardsphere.com/sets/, then downloads
the haves.csv for each set. Produces two DataFrames:

1. cards_df: scryfallId -> cardsphereId / cardsphereFoilId
2. sets_df:  setCode   -> cardsphereSetId
"""

import csv
import io
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mtgjson5 import constants

LOGGER = logging.getLogger(__name__)

SETS_URL = "https://www.cardsphere.com/sets/"
CSV_URL_TEMPLATE = "https://www.cardsphere.com/sets/{set_id}/haves.csv"

# Schema for the card-level mapping
CARD_SCHEMA = {
    "scryfallId": pl.String,
    "cardsphereId": pl.String,
    "cardsphereFoilId": pl.String,
}

# Schema for the set-level mapping
SET_SCHEMA = {
    "cardsphereSetId": pl.Int64,
    "cardsphereSetCode": pl.String,
    "cardsphereSetName": pl.String,
}

# Max concurrent downloads
_MAX_WORKERS = 8


def _build_session() -> requests.Session:
    """Build a requests session with retry logic."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=_MAX_WORKERS + 2)
    session.mount("https://", adapter)
    session.headers["User-Agent"] = "MTGJSON/5.0"
    return session


class CardSphereProvider:
    """Provider for CardSphere card and set ID data."""

    def __init__(self) -> None:
        self._cache_path: Path = constants.CACHE_PATH
        self._cards_df: pl.DataFrame | None = None
        self._sets_df: pl.DataFrame | None = None

    def fetch_and_build(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch all CardSphere data and return (cards_df, sets_df).

        Returns:
            Tuple of (cards_df, sets_df). cards_df maps scryfallId to
            cardsphereId/cardsphereFoilId. sets_df maps set codes to
            cardsphereSetId.
        """
        sets_list = self._fetch_sets_index()
        if not sets_list:
            LOGGER.warning("No sets found on CardSphere, returning empty frames")
            return self._empty_cards_df(), self._empty_sets_df()

        LOGGER.info(f"Found {len(sets_list)} CardSphere sets, downloading CSVs...")

        # Build set-level mapping
        self._sets_df = pl.DataFrame(
            [
                {
                    "cardsphereSetId": s["id"],
                    "cardsphereSetCode": s["code"],
                    "cardsphereSetName": s["name"],
                }
                for s in sets_list
            ],
            schema=SET_SCHEMA,
        )

        # Download all CSVs concurrently and collect rows
        all_rows: list[dict[str, str | None]] = []
        session = _build_session()

        set_ids = [s["id"] for s in sets_list]
        failed = 0

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._download_csv, session, set_id): set_id
                for set_id in set_ids
            }

            for future in as_completed(futures):
                set_id = futures[future]
                try:
                    rows = future.result()
                    all_rows.extend(rows)
                except Exception as e:
                    failed += 1
                    LOGGER.debug(f"Failed to download CSV for set {set_id}: {e}")

        session.close()

        if failed:
            LOGGER.warning(f"Failed to download {failed}/{len(set_ids)} CSVs")

        LOGGER.info(f"Downloaded {len(all_rows):,} card rows from CardSphere")

        if not all_rows:
            self._cards_df = self._empty_cards_df()
            return self._cards_df, self._sets_df

        # Build raw DataFrame from all rows
        raw_df = pl.DataFrame(all_rows)

        # Pivot: for each scryfallId, get the non-foil and foil CS IDs
        self._cards_df = self._pivot_to_card_mapping(raw_df)

        LOGGER.info(
            f"Built CardSphere mapping: {len(self._cards_df):,} unique cards, "
            f"{len(self._sets_df):,} sets"
        )

        return self._cards_df, self._sets_df

    def _fetch_sets_index(self) -> list[dict]:
        """Fetch the sets page and extract the embedded JSON set list."""
        try:
            response = requests.get(SETS_URL, timeout=30, headers={"User-Agent": "MTGJSON/5.0"})
            response.raise_for_status()
        except Exception as e:
            LOGGER.error(f"Failed to fetch CardSphere sets page: {e}")
            return []

        html = response.text

        # Look for __NEXT_DATA__ JSON payload
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if match:
            try:
                next_data = json.loads(match.group(1))
                # Navigate the Next.js data structure to find sets
                props = next_data.get("props", {}).get("pageProps", {})
                sets_list = props.get("sets") or props.get("data", {}).get("sets")
                if sets_list:
                    return sets_list
            except (json.JSONDecodeError, AttributeError):
                pass

        # Fallback: look for JSON array of sets anywhere in the page
        # Pattern: array of objects with id, name, code fields
        matches = re.findall(
            r'\[(?:\s*\{"id":\d+,"name":"[^"]+","code":"[^"]+"\}\s*,?\s*)+\]',
            html,
        )
        for candidate in matches:
            try:
                parsed = json.loads(candidate)
                if parsed and isinstance(parsed, list) and "id" in parsed[0]:
                    return parsed
            except (json.JSONDecodeError, IndexError, TypeError):
                continue

        # Last resort: extract individual set objects
        set_objects = re.findall(
            r'\{"id":(\d+),"name":"([^"]+)","code":"([^"]+)"\}',
            html,
        )
        if set_objects:
            return [
                {"id": int(m[0]), "name": m[1], "code": m[2]}
                for m in set_objects
            ]

        LOGGER.error("Could not parse set data from CardSphere sets page")
        return []

    @staticmethod
    def _download_csv(
        session: requests.Session,
        set_id: int,
    ) -> list[dict[str, str | None]]:
        """Download and parse a single set's CSV.

        Returns list of dicts with keys: scryfallId, cardsphereId, foil.
        """
        url = CSV_URL_TEMPLATE.format(set_id=set_id)
        response = session.get(url, timeout=30)
        response.raise_for_status()

        rows: list[dict[str, str | None]] = []
        reader = csv.DictReader(io.StringIO(response.text))

        for row in reader:
            scryfall_id = row.get("Scryfall ID", "").strip()
            cs_id = row.get("Cardsphere ID", "").strip()
            foil = row.get("Foil", "").strip()

            if not scryfall_id or not cs_id:
                continue

            rows.append(
                {
                    "scryfallId": scryfall_id,
                    "cardsphereId": cs_id,
                    "foil": foil,
                }
            )

        return rows

    @staticmethod
    def _pivot_to_card_mapping(raw_df: pl.DataFrame) -> pl.DataFrame:
        """Pivot raw CSV rows into scryfallId -> cardsphereId / cardsphereFoilId.

        Each scryfallId can have up to two rows: Foil=N and Foil=F.
        We pivot these into separate columns.
        """
        non_foil = (
            raw_df.filter(pl.col("foil") == "N")
            .select(
                pl.col("scryfallId"),
                pl.col("cardsphereId").alias("cardsphereId"),
            )
            .unique(subset=["scryfallId"], keep="first")
        )

        foil = (
            raw_df.filter(pl.col("foil") == "F")
            .select(
                pl.col("scryfallId"),
                pl.col("cardsphereId").alias("cardsphereFoilId"),
            )
            .unique(subset=["scryfallId"], keep="first")
        )

        # Full outer join to capture cards that only exist in one finish
        result = non_foil.join(foil, on="scryfallId", how="full", coalesce=True)

        return result.select(
            pl.col("scryfallId"),
            pl.col("cardsphereId").cast(pl.String),
            pl.col("cardsphereFoilId").cast(pl.String),
        )

    @staticmethod
    def _empty_cards_df() -> pl.DataFrame:
        return pl.DataFrame(schema=CARD_SCHEMA)

    @staticmethod
    def _empty_sets_df() -> pl.DataFrame:
        return pl.DataFrame(schema=SET_SCHEMA)

    @property
    def cards_df(self) -> pl.DataFrame | None:
        return self._cards_df

    @property
    def sets_df(self) -> pl.DataFrame | None:
        return self._sets_df
