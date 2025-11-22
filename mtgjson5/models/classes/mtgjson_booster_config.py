from pydantic import Field

from ..mtgjson_base import MTGJsonModel


class MtgjsonBoosterPackObject(MTGJsonModel):
    """
    Represents a single booster pack configuration within a booster definition.
    """

    contents: dict[str, int] = Field(
        default_factory=dict,
        description="The contents of the pack, mapping code to count.",
    )
    weight: float = Field(
        description="The probability weight of this specific pack configuration."
    )


class MtgjsonBoosterSheetObject(MTGJsonModel):
    """
    Describes a sheet of cards used to construct a booster pack.
    """

    allow_duplicates: bool | None = Field(
        default=None, description="Whether duplicates are allowed on this sheet."
    )
    balance_colors: bool | None = Field(
        default=None,
        description="Whether to balance colors when selecting from this sheet.",
    )
    cards: dict[str, int] = Field(
        default_factory=dict,
        description="The cards on this sheet and their relative weights.",
    )
    foil: bool = Field(description="Whether this sheet contains foil cards.")
    fixed: bool | None = Field(
        default=None, description="Whether this sheet has fixed contents."
    )
    total_weight: float = Field(
        description="The total weight of all cards on the sheet."
    )


class MtgjsonBoosterConfigObject(MTGJsonModel):
    """
    A breakdown of possibilities and weights of cards in a booster pack.
    """

    boosters: list[MtgjsonBoosterPackObject] = Field(
        default_factory=list,
        description="The list of possible booster pack configurations.",
    )
    boosters_total_weight: float = Field(
        description="The total weight of all booster pack configurations."
    )
    name: str | None = Field(
        default=None, description="The name of the booster configuration."
    )
    sheets: dict[str, MtgjsonBoosterSheetObject] = Field(
        default_factory=dict, description="The sheets used to generate the boosters."
    )
    source_set_codes: list[str] = Field(
        default_factory=list,
        description="The set codes where these cards are sourced from.",
    )
