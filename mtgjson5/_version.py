"""Dynamic version read from mtgjson.properties."""

import configparser
import pathlib

_config = configparser.ConfigParser()
_config.read(pathlib.Path(__file__).parent / "resources" / "mtgjson.properties")
__version__ = _config.get("MTGJSON", "version", fallback="5.3.0+fallback")
