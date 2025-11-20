from ..mtgjson_base import MTGJsonModel


class MtgjsonRulingObject(MTGJsonModel):
    """
    MTGJSON Singular Card.Rulings Object
    """

    date: str
    text: str

    def __init__(self, date: str, text: str, **data):
        """
        Set the ruling date and text
        :param date: Ruling date
        :param text: Ruling text
        """
        super().__init__(date=date, text=text, **data)
