"""
MTGJSON Configuration Service
"""

import configparser
import logging
import pathlib
from typing import Optional

import boto3
import botocore.exceptions
from singleton_decorator import singleton

from . import constants


@singleton
class MtgjsonConfig:
    """
    Configuration Class that loads in the appropriate configuration file
    and provides the contents for the running program
    """

    logger: logging.Logger
    config_parser: configparser.ConfigParser
    mtgjson_version: str
    use_cache: bool
    output_path: pathlib.Path
    vectorized: bool
    use_bulk_for_searches: bool

    def __init__(
        self,
        aws_ssm_config_name: Optional[str] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.config_parser = configparser.ConfigParser()
        if aws_ssm_config_name:
            self.logger.info("Loading configuration from AWS SSM")
            self.__load_config_from_aws_ssm(aws_ssm_config_name)
        else:
            self.logger.info("Loading configuration from local file")
            self.__load_config_from_local_file(constants.CONFIG_PATH)

        try:
            self.mtgjson_version = self.config_parser.get(
                "MTGJSON", "version", fallback="NO_VERSION_FOUND"
            )
            if aws_ssm_config_name:
                self.mtgjson_version += (
                    f"+{constants.MTGJSON_BUILD_DATE.replace('-', '')}"
                )
        except configparser.NoSectionError:
            self.logger.warning(
                "Key 'version' is missing from Section 'MTGJSON' in config file"
            )
            self.mtgjson_version = (
                f"5.X.X+{constants.MTGJSON_BUILD_DATE.replace('-', '')}"
            )

        self.use_cache = self.get_boolean("MTGJSON", "use_cache", False)
        self.use_bulk_for_searches = False  # Set by --polars or --bulk-files flags
        self.output_path = constants.ENV_OUT_PATH.joinpath(
            f"mtgjson_build_{self.mtgjson_version}"
        )

    def __load_config_from_aws_ssm(self, config_name: str) -> None:
        """
        Load AWS SSM SecureString as MTGJSON configuration file
        :param config_name: AWS SSM key name
        """
        try:
            ssm = boto3.client("ssm")
            parameter = ssm.get_parameter(Name=config_name, WithDecryption=True)
        except botocore.exceptions.ClientError as error:
            self.logger.fatal(f"Unable to download {config_name} from SSM", error)
            raise error

        self.config_parser.read_string(parameter["Parameter"]["Value"])

    def __load_config_from_local_file(self, file_path: pathlib.Path) -> None:
        """
        Load local file from resources as MTGJSON configuration file
        :param file_path: Path to Configuration file
        """
        self.config_parser.read(str(file_path))

    def get(self, section: str, option: str, fallback: str = "") -> str:
        """
        Get a specific value from configuration
        :param section: Section header
        :param option: Key in section
        :param fallback: Default value to use if key not found in section
        :returns Configuration value to use
        """
        if self.has_option(section, option):
            return self.config_parser.get(section, option, fallback=fallback)
        return fallback

    def get_boolean(self, section: str, option: str, fallback: bool = False) -> bool:
        """
        Get a specific value from configuration
        :param section: Section header
        :param option: Key in section
        :param fallback: Default value to use if key not found in section
        :returns Configuration value to use (as a Boolean)
        """
        if self.has_option(section, option):
            return self.config_parser.getboolean(section, option, fallback=fallback)
        return fallback

    def has_section(self, section: str) -> bool:
        """
        Check if Configuration has a specific section
        :param section: Section header to find
        :return Does Section header exist
        """
        return self.config_parser.has_section(section)

    def has_option(self, section: str, option: str) -> bool:
        """
        Check if Configuration has a specific option in a specific section
        and has a defined value (ala not VAR=)
        :param section: Section header to find
        :param option: Option to find in section
        :return Does option exist in section
        """
        return (
            self.config_parser.has_option(section, option)
            and len(str(self.config_parser.get(section, option))) > 0
        )
