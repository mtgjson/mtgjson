"""
MTGJSON Singular Card.PurchaseURLs Object
"""

from collections.abc import Iterable

from .json_object import JsonObject


class MtgjsonPurchaseUrlsObject(JsonObject):
	"""
	MTGJSON Singular Card.PurchaseURLs Object
	"""

	card_kingdom: str
	card_kingdom_etched: str
	card_kingdom_foil: str
	cardmarket: str
	tcgplayer: str
	tcgplayer_etched: str

	def build_keys_to_skip(self) -> Iterable[str]:
		"""
		Build this object's instance of what keys to skip under certain circumstances
		:return What keys to skip over
		"""
		excluded_keys = set()

		for _, value in self.__dict__.items():
			if not value:
				excluded_keys.add(value)

		return excluded_keys
