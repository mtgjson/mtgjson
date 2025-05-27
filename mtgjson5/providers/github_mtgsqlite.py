"""
MTGSqlite via GitHub 3rd party provider
"""

import logging
import pathlib
import shutil
import subprocess
import sys
from typing import Any, Dict, Optional, Union

import git
from singleton_decorator import singleton

from .. import constants
from ..compiled_classes.mtgjson_structures import MtgjsonStructuresObject
from ..mtgjson_config import MtgjsonConfig
from ..providers.abstract import AbstractProvider

LOGGER = logging.getLogger(__name__)


@singleton
class GitHubMTGSqliteProvider(AbstractProvider):
    """
    GitHubMTGSqliteProvider container
    """

    repo_url: str = "https://github.com/mtgjson/mtgsqlive/"
    all_printings_file: pathlib.Path = MtgjsonConfig().output_path.joinpath(
        f"{MtgjsonStructuresObject().all_printings}.json"
    )
    temp_download_path: pathlib.Path = constants.CACHE_PATH.joinpath("GitHub-MTGSQLive")

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
        __github_token = MtgjsonConfig().get("GitHub", "api_token")
        return {"Authorization": f"Bearer {__github_token}"}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
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

    @staticmethod
    def build_alternative_formats() -> None:
        """
        Call to external MTGSqlive to compile CSV and SQL output files
        """
        LOGGER.info("Building MTGSQLive...")
        try:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "mtgsqlive",
                    "-i",
                    MtgjsonConfig().output_path,
                    "-o",
                    MtgjsonConfig().output_path,
                    "--all",
                ],
            )
        except subprocess.CalledProcessError as error:
            LOGGER.error(f"Error building MTGSQLive: {error}")
