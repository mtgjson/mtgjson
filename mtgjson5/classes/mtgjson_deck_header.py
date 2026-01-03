"""
MTGJSON Singular Deck Header Object
"""

from ..classes.mtgjson_deck import MtgjsonDeckObject
from .json_object import JsonObject


class MtgjsonDeckHeaderObject(JsonObject):
	"""
	MTGJSON Singular Deck Header Object
	"""

	code: str
	file_name: str
	name: str
	release_date: str
	type: str

	def __init__(self, output_deck: MtgjsonDeckObject) -> None:
		"""
		Initialize the header given a deck
		"""
		self.code = output_deck.code
		self.file_name = output_deck.file_name
		self.name = output_deck.name
		self.release_date = output_deck.release_date
		self.type = output_deck.type
