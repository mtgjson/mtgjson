from typing import Literal, Optional
from uuid import UUID
from pydantic.dataclasses import BaseModel, Field, dataclass

@dataclass
class CardFace(BaseModel):
    """
    A single face of a multiface card.

    Multiface cards have a card_faces property containing at least two
    Card Face objects.
    """

    object: Literal["card_face"] = Field(
        default="card_face",
        description="A content type for this object, always card_face.",
    )
    name: str = Field(
        description="The name of this particular face.",
    )
    mana_cost: str = Field(
        description=(
            "The mana cost for this face. This value will be any empty string "
            "if the cost is absent. Remember that per the game rules, a missing "
            "mana cost and a mana cost of {0} are different values."
        ),
    )
    type_line: Optional[str] = Field(
        default=None,
        description="The type line of this particular face, if the card is reversible.",
    )
    oracle_text: Optional[str] = Field(
        default=None,
        description="The Oracle text for this face, if any.",
    )
    colors: Optional[list[str]] = Field(
        default=None,
        description=(
            "This face's colors, if the game defines colors for the individual "
            "face of this card."
        ),
    )
    color_indicator: Optional[list[str]] = Field(
        default=None,
        description="The colors in this face's color indicator, if any.",
    )
    power: Optional[str] = Field(
        default=None,
        description=(
            "This face's power, if any. Note that some cards have powers that "
            "are not numeric, such as *."
        ),
    )
    toughness: Optional[str] = Field(
        default=None,
        description=(
            "This face's toughness, if any. Note that some cards have toughnesses "
            "that are not numeric, such as *."
        ),
    )
    defense: Optional[str] = Field(
        default=None,
        description="This face's defense, if any.",
    )
    loyalty: Optional[str] = Field(
        default=None,
        description="This face's loyalty, if any.",
    )
    flavor_text: Optional[str] = Field(
        default=None,
        description="The flavor text printed on this face, if any.",
    )
    illustration_id: Optional[UUID] = Field(
        default=None,
        description=(
            "A unique identifier for the card face artwork that remains consistent "
            "across reprints. Newly spoiled cards may not have this field yet."
        ),
    )
    image_uris: Optional[str] = Field(
        default=None,
        description=(
            "An object providing URIs to imagery for this face, if this is a "
            "double-sided card. If this card is not double-sided, then the "
            "image_uris property will be part of the parent object instead."
        ),
    )
    artist: Optional[str] = Field(
        default=None,
        description=(
            "The name of the illustrator of this card face. Newly spoiled cards "
            "may not have this field yet."
        ),
    )
    artist_id: Optional[UUID] = Field(
        default=None,
        description=(
            "The ID of the illustrator of this card face. Newly spoiled cards "
            "may not have this field yet."
        ),
    )
    watermark: Optional[str] = Field(
        default=None,
        description="The watermark on this particular card face, if any.",
    )
    printed_name: Optional[str] = Field(
        default=None,
        description="The localized name printed on this face, if any.",
    )
    printed_text: Optional[str] = Field(
        default=None,
        description="The localized text printed on this face, if any.",
    )
    printed_type_line: Optional[str] = Field(
        default=None,
        description="The localized type line printed on this face, if any.",
    )
    cmc: Optional[float] = Field(
        default=None,
        description="The mana value of this particular face, if the card is reversible.",
    )
    oracle_id: Optional[UUID] = Field(
        default=None,
        description="The Oracle ID of this particular face, if the card is reversible.",
    )
    layout: Optional[str] = Field(
        default=None,
        description="The layout of this card face, if the card is reversible.",
    )
    
    def get_schema(self) -> dict:
        """Return the schema representation of this dataclass."""
        return self.__class__.model_json_schema()