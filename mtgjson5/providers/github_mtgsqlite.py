"""
MTGSqlite via GitHub 3rd party provider
"""

import logging
import pathlib
import shutil
import subprocess
import sys
from typing import Any, Dict, Union

import git
from singleton_decorator import singleton

from ..compiled_classes.mtgjson_structures import MtgjsonStructuresObject
from ..consts import CACHE_PATH, OUTPUT_PATH
from ..providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubMTGSqliteProvider(AbstractProvider):
    """
    GitHubMTGSqliteProvider container
    """

    repo_url: str = "https://github.com/mtgjson/mtgsqlive/"
    all_printings_file: pathlib.Path = OUTPUT_PATH.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    )
    temp_download_path: pathlib.Path = CACHE_PATH.joinpath("GitHub-MTGSQLive")

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())
        self.download(self.repo_url)

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return dict()

    def download(self, url: str, params: Dict[str, Union[str, int]] = None) -> Any:
        """
        Download content from GitHub
        :param url: Download URL
        :param params: Options for URL download
        """
        if self.temp_download_path.is_dir():
            shutil.rmtree(self.temp_download_path, ignore_errors=True)

        git_sh = git.cmd.Git()
        git_sh.clone(url, self.temp_download_path, depth=1)
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", self.temp_download_path]
            )
        except subprocess.CalledProcessError as error:
            LOGGER.error(f"Unable to pip install: {error}")

    def build_sql_and_csv_files(self) -> None:
        """
        Call to external MTGSqlive to compile CSV and SQL output files
        """
        LOGGER.info("Building MTGSQLive (CSV & SQL)...")
        try:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "mtgsqlive",
                    "-i",
                    self.all_printings_file,
                    "-o",
                    OUTPUT_PATH,
                    "--all",
                ],
            )
        except subprocess.CalledProcessError as error:
            LOGGER.error(f"Error building MTGSQLive: {error}")
