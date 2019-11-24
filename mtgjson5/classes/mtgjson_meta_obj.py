"""
MTGJSON's meta object to determine time and version
"""


class MtgjsonMetaObject:
    """
    Determine what version of software built this object
    """

    date: str
    prices_date: str
    version: str

    def __init__(self):
        pass
