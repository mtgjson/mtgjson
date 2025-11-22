"""MTGJSON Legalities Object model for format legality."""

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonLegalitiesObject(MTGJsonModel):
    """
    The Legalities Data Model describes the properties of legalities of a card in various game play formats.
    """

    alchemy: str | None = Field(
        default=None, description="Legality of the card in the Alchemy play format."
    )
    brawl: str | None = Field(
        default=None, description="Legality of the card in the Brawl play format."
    )
    commander: str | None = Field(
        default=None, description="Legality of the card in the Commander play format."
    )
    duel: str | None = Field(
        default=None,
        description="Legality of the card in the Duel Commander play format.",
    )
    explorer: str | None = Field(
        default=None, description="Legality of the card in the Explorer play format."
    )
    future: str | None = Field(
        default=None,
        description="Legality of the card in the future for the Standard play format.",
    )
    gladiator: str | None = Field(
        default=None, description="Legality of the card in the Gladiator play format."
    )
    historic: str | None = Field(
        default=None, description="Legality of the card in the Historic play format."
    )
    historicbrawl: str | None = Field(
        default=None,
        description="Legality of the card in the Historic Brawl play format.",
    )
    legacy: str | None = Field(
        default=None, description="Legality of the card in the Legacy play format."
    )
    modern: str | None = Field(
        default=None, description="Legality of the card in the Modern play format."
    )
    oathbreaker: str | None = Field(
        default=None, description="Legality of the card in the Oathbreaker play format."
    )
    oldschool: str | None = Field(
        default=None, description="Legality of the card in the Old School play format."
    )
    pauper: str | None = Field(
        default=None, description="Legality of the card in the Pauper play format."
    )
    paupercommander: str | None = Field(
        default=None,
        description="Legality of the card in the Pauper Commander play format.",
    )
    penny: str | None = Field(
        default=None,
        description="Legality of the card in the Penny Dreadful play format.",
    )
    pioneer: str | None = Field(
        default=None, description="Legality of the card in the Pioneer play format."
    )
    predh: str | None = Field(
        default=None, description="Legality of the card in the PreDH play format."
    )
    premodern: str | None = Field(
        default=None, description="Legality of the card in the Premodern play format."
    )
    standard: str | None = Field(
        default=None, description="Legality of the card in the Standard play format."
    )
    standardbrawl: str | None = Field(
        default=None,
        description="Legality of the card in the Standard Brawl play format.",
    )
    timeless: str | None = Field(
        default=None, description="Legality of the card in the Timeless play format."
    )
    vintage: str | None = Field(
        default=None, description="Legality of the card in the Vintage play format."
    )
