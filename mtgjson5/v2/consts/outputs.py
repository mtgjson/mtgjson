"""
Output file and directory name constants.

These are the canonical names for MTGJSON output files and archive directories.
"""

from __future__ import annotations

from typing import Final

# Archive directory names (for zip/tar archives)
ALL_SETS_DIRECTORY: Final[str] = "AllSetFiles"
ALL_DECKS_DIRECTORY: Final[str] = "AllDeckFiles"
ALL_CSVS_DIRECTORY: Final[str] = "AllPrintingsCSVFiles"
ALL_PARQUETS_DIRECTORY: Final[str] = "AllPrintingsParquetFiles"
