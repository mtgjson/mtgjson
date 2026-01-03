"""
MTGJSON CompiledList Object
"""

from ..classes.json_object import JsonObject
from .mtgjson_structures import MtgjsonStructuresObject


class MtgjsonCompiledListObject(JsonObject):
	"""
	MTGJSON CompiledList Object
	"""

	files: list[str]

	def __init__(self) -> None:
		"""
		Initializer to build up the object
		"""
		self.files = sorted(MtgjsonStructuresObject().get_compiled_list_files())

	def to_json(self) -> list[str]:
		"""
		Support json.dump()
		:return: JSON serialized object
		"""
		return self.files
