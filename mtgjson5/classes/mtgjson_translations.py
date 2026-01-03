"""
MTGJSON Set.Translations Object
"""

from typing import Any

from mtgjson5.classes.json_object import JsonObject


class MtgjsonTranslationsObject(JsonObject):
	"""
	MTGJSON Set.Translations Object
	"""

	chinese_simplified: str | None
	chinese_traditional: str | None
	french: str | None
	german: str | None
	italian: str | None
	japanese: str | None
	korean: str | None
	portuguese_ob_brazil_cb: str | None
	russian: str | None
	spanish: str | None

	def __init__(self, active_dict: dict[str, str] | None = None) -> None:
		"""
		Initializer, for each language, given the contents
		"""
		if not active_dict:
			return

		self.chinese_simplified = active_dict.get("Chinese Simplified")
		self.chinese_traditional = active_dict.get("Chinese Traditional")
		self.french = active_dict.get("French", active_dict.get("fr"))
		self.german = active_dict.get("German", active_dict.get("de"))
		self.italian = active_dict.get("Italian", active_dict.get("it"))
		self.japanese = active_dict.get("Japanese")
		self.korean = active_dict.get("Korean")
		self.portuguese_ob_brazil_cb = active_dict.get("Portuguese (Brazil)")
		self.russian = active_dict.get("Russian")
		self.spanish = active_dict.get("Spanish", active_dict.get("es"))

	@staticmethod
	def parse_key(key: str) -> str:
		"""
		Custom parsing of translation keys
		:param key: Key to translate
		:return: Translated key for JSON
		"""
		key = key.replace("ob_", "(").replace("_cb", ")")
		components = key.split("_")
		return " ".join(x.title() for x in components)

	def to_json(self) -> dict[str, Any]:
		return {
			self.parse_key(key): value
			for key, value in self.__dict__.items()
			if "__" not in key and not callable(value)
		}
