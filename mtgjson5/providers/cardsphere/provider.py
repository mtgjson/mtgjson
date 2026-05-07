"""
CardSphere provider - downloads set CSVs and builds ID mappings.

Fetches the set index from cardsphere.com/sets/, then downloads
the haves.csv for each set. Produces two DataFrames:

1. cards_df: scryfallId -> cardsphereId / cardsphereFoilId / cardsphereEtchedId
2. sets_df:  setCode   -> cardsphereSetId
"""

import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
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
    "cardsphereEtchedId": pl.String,
}

# Schema for the set-level mapping
SET_SCHEMA = {
    "cardsphereSetId": pl.Int64,
    "cardsphereSetCode": pl.String,
    "cardsphereSetName": pl.String,
}

# Max concurrent downloads
_MAX_WORKERS = 8


class _NextDataParser(HTMLParser):
    """Extract the __NEXT_DATA__ script content from HTML."""

    def __init__(self) -> None:
        super().__init__()
        self._in_next_data = False
        self._data: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script":
            attr_dict = dict(attrs)
            if attr_dict.get("id") == "__NEXT_DATA__":
                self._in_next_data = True

    def handle_data(self, data: str) -> None:
        if self._in_next_data:
            self._data = data

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._in_next_data:
            self._in_next_data = False

    @property
    def content(self) -> str | None:
        return self._data


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

    def fetch_and_build(
        self,
        finishes_df: pl.DataFrame | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch all CardSphere data and return (cards_df, sets_df).

        Args:
            finishes_df: Optional DataFrame with columns [scryfallId, finishes]
                where finishes is a list of strings (e.g. ["nonfoil", "foil"]).
                Used to distinguish foil vs etched CardSphere IDs since CS
                marks both as Foil=F.

        Returns:
            Tuple of (cards_df, sets_df). cards_df maps scryfallId to
            cardsphereId/cardsphereFoilId/cardsphereEtchedId. sets_df maps
            set codes to cardsphereSetId.
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

        # Download all CSVs concurrently, retrying failures up to 3 times
        frames: list[pl.DataFrame] = []
        pending_ids = [s["id"] for s in sets_list]
        max_attempts = 3

        with _build_session() as session:
            for attempt in range(1, max_attempts + 1):
                failed_ids: list[int] = []

                with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                    futures = {executor.submit(self._download_csv, session, set_id): set_id for set_id in pending_ids}

                    for future in as_completed(futures):
                        set_id = futures[future]
                        try:
                            df = future.result()
                            if len(df) > 0:
                                frames.append(df)
                        except Exception as e:
                            failed_ids.append(set_id)
                            LOGGER.debug(
                                f"Failed to download CSV for set {set_id} (attempt {attempt}/{max_attempts}): {e}"
                            )

                if not failed_ids:
                    break

                pending_ids = failed_ids
                if attempt < max_attempts:
                    LOGGER.info(f"Retrying {len(failed_ids)} failed CSVs (attempt {attempt + 1}/{max_attempts})")

        if failed_ids:
            LOGGER.warning(
                f"Failed to download {len(failed_ids)}/{len(sets_list)} CSVs after {max_attempts} attempts: "
                f"set_ids={failed_ids[:20]}{'...' if len(failed_ids) > 20 else ''}"
            )

        if not frames:
            LOGGER.warning("No card data retrieved from CardSphere")
            self._cards_df = self._empty_cards_df()
            return self._cards_df, self._sets_df

        raw_df = pl.concat(frames)
        LOGGER.info(f"Downloaded {len(raw_df):,} card rows from CardSphere")

        # Pivot: for each scryfallId, get the non-foil, foil, and etched CS IDs
        self._cards_df = self._pivot_to_card_mapping(raw_df, finishes_df)

        LOGGER.info(f"Built CardSphere mapping: {len(self._cards_df):,} unique cards, {len(self._sets_df):,} sets")

        return self._cards_df, self._sets_df

    def _fetch_sets_index(self) -> list[dict]:
        """Fetch the sets page and extract the embedded JSON set list."""
        try:
            response = requests.get(SETS_URL, timeout=30, headers={"User-Agent": "MTGJSON/5.0"})
            response.raise_for_status()
        except Exception as e:
            LOGGER.error(f"Failed to fetch CardSphere sets page: {e}")
            return []

        return self._parse_sets_from_html(response.text)

    @staticmethod
    def _parse_sets_from_html(html: str) -> list[dict]:
        """Extract the sets list from a CardSphere HTML page.

        Uses html.parser to locate the __NEXT_DATA__ script tag and
        parse its JSON content.
        """
        parser = _NextDataParser()
        parser.feed(html)

        if not parser.content:
            LOGGER.error("Could not find __NEXT_DATA__ script tag on CardSphere sets page")
            return []

        try:
            next_data = json.loads(str(parser.content))
        except json.JSONDecodeError as e:
            LOGGER.error(f"Failed to parse __NEXT_DATA__ JSON: {e}")
            return []

        props = next_data.get("props", {}).get("pageProps", {})
        sets_list = props.get("sets") or props.get("data", {}).get("sets")

        if not sets_list:
            LOGGER.error("__NEXT_DATA__ found but no sets data in pageProps")
            return []

        return list(sets_list)

    @staticmethod
    def _download_csv(
        session: requests.Session,
        set_id: int,
    ) -> pl.DataFrame:
        """Download and parse a single set's CSV using polars.

        Returns a DataFrame with columns: scryfallId, cardsphereId, foil.
        """
        url = CSV_URL_TEMPLATE.format(set_id=set_id)
        response = session.get(url, timeout=30)
        response.raise_for_status()

        df = pl.read_csv(
            io.BytesIO(response.content),
            columns=["Scryfall ID", "Cardsphere ID", "Foil"],
            schema_overrides={
                "Scryfall ID": pl.String,
                "Cardsphere ID": pl.String,
                "Foil": pl.String,
            },
        )

        return (
            df.rename(
                {
                    "Scryfall ID": "scryfallId",
                    "Cardsphere ID": "cardsphereId",
                    "Foil": "foil",
                }
            )
            .filter(pl.col("scryfallId").str.len_bytes() > 0)
            .filter(pl.col("cardsphereId").str.len_bytes() > 0)
        )

    @staticmethod
    def _pivot_to_card_mapping(
        raw_df: pl.DataFrame,
        finishes_df: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Pivot raw CSV rows into scryfallId -> cardsphereId / cardsphereFoilId / cardsphereEtchedId.

        CardSphere marks both foil and etched cards as Foil=F. We use Scryfall's
        finishes data to determine whether an F row is foil or etched:
        - If finishes contains "etched" but not "foil": F -> cardsphereEtchedId
        - If finishes contains "foil" (regardless of etched): F -> cardsphereFoilId
        - If no finishes data available: F -> cardsphereFoilId (default)
        """
        # Non-foil is straightforward
        non_foil = (
            raw_df.filter(pl.col("foil") == "N")
            .select("scryfallId", pl.col("cardsphereId"))
            .unique(subset=["scryfallId"], keep="first")
        )

        # Split F rows into foil vs etched using Scryfall finishes
        foil_rows = raw_df.filter(pl.col("foil") == "F")

        if finishes_df is not None and len(foil_rows) > 0:
            # Build a lookup: scryfallIds whose finishes include "etched" but NOT "foil"
            etched_only_ids = finishes_df.filter(
                pl.col("finishes").list.contains("etched") & ~pl.col("finishes").list.contains("foil")
            ).select("scryfallId")

            # F rows for etched-only cards -> cardsphereEtchedId
            etched = (
                foil_rows.join(etched_only_ids, on="scryfallId", how="semi")
                .select("scryfallId", pl.col("cardsphereId").alias("cardsphereEtchedId"))
                .unique(subset=["scryfallId"], keep="first")
            )

            # F rows for everything else -> cardsphereFoilId
            foil = (
                foil_rows.join(etched_only_ids, on="scryfallId", how="anti")
                .select("scryfallId", pl.col("cardsphereId").alias("cardsphereFoilId"))
                .unique(subset=["scryfallId"], keep="first")
            )
        else:
            # No finishes data: all F rows default to cardsphereFoilId
            foil = foil_rows.select("scryfallId", pl.col("cardsphereId").alias("cardsphereFoilId")).unique(
                subset=["scryfallId"], keep="first"
            )
            etched = pl.DataFrame(schema={"scryfallId": pl.String, "cardsphereEtchedId": pl.String})

        # Full outer joins to capture cards that only exist in one finish
        result = non_foil.join(foil, on="scryfallId", how="full", coalesce=True).join(
            etched, on="scryfallId", how="full", coalesce=True
        )

        return result.select("scryfallId", "cardsphereId", "cardsphereFoilId", "cardsphereEtchedId")

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
