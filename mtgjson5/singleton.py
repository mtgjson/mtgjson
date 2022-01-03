"""
Singleton Class Definition
"""
from typing import Any, Dict


class Singleton(type):
    """
    Singleton Metaclass
    """

    _instances: Dict[Any, "Singleton"] = {}

    def __call__(cls, *args: Any, **kwargs: Any) -> "Singleton":
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
