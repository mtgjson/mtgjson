"""EDHREC Salt/Rank Provider."""

import logging
from functools import cached_property

import polars as pl
import requests

from mtgjson5 import constants

LOGGER = logging.getLogger(__name__)

EDHREC_CARDRANKS_URL = "https://edhrec.com/data/cardranks.json"


class EdhrecSaltProvider:
    """Provider for EDHREC card rank and salt data."""

    def __init__(self) -> None:
        self._cache_path: str = constants.CACHE_PATH / "salt.json"
        self._df: pl.LazyFrame | None = None

    @cached_property
    def df(self) -> pl.DataFrame:
        """Fetch and return as DataFrame."""
        if self._df is not None:
            return self._df
        
        if self._cache_path.exists():
            LOGGER.info("Loading EDHREC card ranks from cache...")
            self._df = pl.read_json(self._cache_path).select([
                pl.col("name"),
                pl.col("edhrecSaltiness"),
                pl.col("edhrecRank"),
                pl.col("oracle_id"),
            ])
            LOGGER.info(f"Loaded {len(self._df)} cards from cache")
            return self._df
        else:      
            LOGGER.info("Fetching EDHREC card ranks...")
            try:
                response = requests.get(EDHREC_CARDRANKS_URL, timeout=30)
                response.raise_for_status()
                data = response.json()

                self._df = pl.DataFrame(data).select([
                    pl.col("name"),
                    pl.col("salt").alias("edhrecSaltiness"),
                    pl.col("rank").alias("edhrecRank"),
                    pl.col("oracle_id"),
                ])

                LOGGER.info(f"Loaded {len(self._df)} cards from EDHREC")
                return self._df
            except Exception as e:
                LOGGER.warning(f"Failed to fetch EDHREC data: {e}")
                return pl.LazyFrame(schema={
                    "name": pl.String,
                    "edhrecSaltiness": pl.Float64,
                    "edhrecRank": pl.Int64,
                    "oracle_id": pl.String,
                })
        
    def get_data_frame(self) -> pl.LazyFrame:
        """Get the DataFrame of EDHREC salt/rank data."""
        return self.df.lazy()