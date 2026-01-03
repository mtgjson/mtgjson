"""
MTGJSON Singular Card.GameFormats Object
"""

from collections.abc import Iterable

from .json_object import JsonObject


class MtgjsonGameFormatsObject(JsonObject):
	"""
	MTGJSON Singular Card.GameFormats Object
	"""

	paper: bool
	mtgo: bool
	arena: bool
	shandalar: bool
	dreamcast: bool

	def __init__(self) -> None:
		"""
		Empty initializer
		"""

	def to_json(self) -> Iterable[str]:
		parent = super().to_json()
		return [key for key, value in parent.items() if value]
