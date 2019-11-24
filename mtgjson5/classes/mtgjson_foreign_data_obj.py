"""
MTGJSON container for foreign entries
"""


class MtgjsonForeignDataObject:
    """
    Foreign data rows
    """

    flavor_text: str
    language: str
    multiverse_id: int
    name: str
    text: str
    type: str

    def __init__(self):
        pass
