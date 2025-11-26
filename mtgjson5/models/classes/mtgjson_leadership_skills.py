"""MTGJSON Leadership Skills Object model for commander eligibility."""

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonLeadershipSkillsObject(MTGJsonModel):
    """
    The Leadership Skills Data Model describes the properties of formats that a card is legal to be your Commander.
    """

    brawl: bool = Field(
        default=False,
        description="If the card can be your commander in the Standard Brawl format.",
    )
    commander: bool = Field(
        default=False,
        description="If the card can be your commander in the Commander/EDH format.",
    )
    oathbreaker: bool = Field(
        default=False,
        description="If the card can be your commander in the Oathbreaker format.",
    )
