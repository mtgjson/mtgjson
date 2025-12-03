import pathlib
from typing import Optional

from mtgjson5 import constants


class GlobalCache:
    """Global shared access cache for provider data."""

    _instance: Optional["GlobalCache"] = None

    def __new__(cls) -> "GlobalCache":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        
        self.CACHE_DIR: pathlib.Path = constants.CACHE_PATH
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        self._initialized = True
        
        @classmethod
        def get_instance(cls) -> "GlobalCache":
            if cls._instance is None:
                cls()
            return cls._instance


GLOBAL_CACHE = GlobalCache.get_instance()