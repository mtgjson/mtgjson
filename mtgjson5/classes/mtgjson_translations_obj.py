"""
MTGJSON container for Set Translations
"""


class MtgjsonTranslationsObject:
    """
    Structure to hold translations for an individual set
    """

    chinese_simplified: str
    chinese_traditional: str
    french: str
    german: str
    italian: str
    japanese: str
    korean: str
    portuguese_brazil: str
    russian: str
    spanish: str

    def __init__(self):
        pass
