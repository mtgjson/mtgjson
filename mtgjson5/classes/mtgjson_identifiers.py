"""
MTGJSON Singular Card.Identifiers Object
"""

from .json_object import JsonObject


class MtgjsonIdentifiersObject(JsonObject):
	"""
	MTGJSON Singular Card.Identifiers Object
	"""

	card_kingdom_etched_id: str | None
	card_kingdom_foil_id: str | None
	card_kingdom_id: str | None
	cardsphere_foil_id: str | None
	cardsphere_id: str | None
	mcm_id: str | None
	mcm_meta_id: str | None
	mtg_arena_id: str | None
	mtgjson_foil_version_id: str | None
	mtgjson_non_foil_version_id: str | None
	mtgjson_v4_id: str | None
	mtgo_foil_id: str | None
	mtgo_id: str | None
	multiverse_id: str | None
	scryfall_id: str | None
	scryfall_illustration_id: str | None
	scryfall_card_back_id: str | None
	scryfall_oracle_id: str | None
	tcgplayer_etched_product_id: str | None
	tcgplayer_product_id: str | None

	def __init__(self) -> None:
		"""
		Empty initializer
		"""
		self.multiverse_id = ""
		self.card_kingdom_id = ""
		self.tcgplayer_product_id = ""

	def to_json(self) -> dict[str, str]:
		parent = super().to_json()
		return {key: value for key, value in parent.items() if value}
