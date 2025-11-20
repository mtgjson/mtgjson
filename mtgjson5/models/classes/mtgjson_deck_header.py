from ..mtgjson_base import MTGJsonModel
from .mtgjson_deck import MtgjsonDeckObject


class MtgjsonDeckHeaderObject(MTGJsonModel):
    """
    MTGJSON Singular Deck Header Object
    """

    code: str = ""
    file_name: str = ""
    name: str = ""
    release_date: str = ""
    type: str = ""

    def __init__(self, output_deck: MtgjsonDeckObject = None, **data):
        """
        Initialize deck header from deck object
        :param output_deck: Deck object to extract header from
        """
        if output_deck:
            data.update({
                'code': output_deck.code,
                'file_name': output_deck.file_name,
                'name': output_deck.name,
                'release_date': output_deck.release_date,
                'type': output_deck.type,
            })
        super().__init__(**data)
