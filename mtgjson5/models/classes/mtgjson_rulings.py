"""MTGJSON Rulings Object model for official card rulings."""

from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonRulingObject(MTGJsonModel):
    """
    The Rulings Data Model describes the properties of rulings for a card.
    """

    date: str = Field(
        validation_alias="published_at",
        description="The release date in ISO 8601 format for the rule.",
    )
    text: str = Field(
        validation_alias="comment",
        description="The text ruling of the card.",
    )
