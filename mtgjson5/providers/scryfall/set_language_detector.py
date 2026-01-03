"""Scryfall provider for detecting available printing languages per set."""

import logging
import time
from typing import Any

import requests
from singleton_decorator import singleton

from ...constants import LANGUAGE_MAP
from ...providers.abstract import AbstractProvider
from ...providers.scryfall import sf_utils


LOGGER = logging.getLogger(__name__)


@singleton
class ScryfallProviderSetLanguageDetector(AbstractProvider):
	"""Provider to detect which languages a set was printed in via Scryfall API."""

	FIRST_CARD_URL = "https://api.scryfall.com/cards/search?q=set:{}&unique=prints&include_extras=true"
	LANG_QUERY_URL = (
		'https://api.scryfall.com/cards/search?q=set:{}%20number:"{}"%20lang:any&unique=prints&include_extras=true'
	)

	def __init__(self) -> None:
		super().__init__(self._build_http_header())

	def _build_http_header(self) -> dict[str, str]:
		return sf_utils.build_http_header()

	def download(
		self,
		url: str,
		params: dict[str, str | int] | None = None,
		retry_ttl: int = 3,
	) -> Any:
		try:
			response = self.session.get(url)
			self.log_download(response)
		except requests.exceptions.ChunkedEncodingError as error:
			if retry_ttl:
				LOGGER.warning(f"Download failed: {error}... Retrying")
				time.sleep(3 - retry_ttl)
				return self.download(url, params, retry_ttl - 1)

			LOGGER.error(f"Download failed: {error}... Maxed out retries")
			return {}

		try:
			return response.json()
		except requests.exceptions.JSONDecodeError as exception:
			LOGGER.error(f"Unable to return {url} with {params} response: {response.text} exception: {exception}")
			return None

	def get_set_printing_languages(self, set_code: str) -> list[str]:
		"""Get the list of languages a set was printed in."""
		first_card_response = self.download(self.FIRST_CARD_URL.format(set_code))

		if not first_card_response or first_card_response.get("object") != "list":
			LOGGER.warning(
				f"Unable to get set printing languages for {set_code} due to bad response: {first_card_response}"
			)
			return []
		if first_card_response.get("total_cards", 0) < 1:
			LOGGER.warning(
				f"Unable to get set printing languages for {set_code} due to no cards in set: {first_card_response}"
			)
			return []

		for entry_to_use in first_card_response["data"]:
			if entry_to_use["name"].startswith("A-"):
				# We don't want to use Arena-only rebalanced cards. They are English only.
				continue
			first_card_number = entry_to_use.get("collector_number")
			break
		else:
			first_card_number = 0

		lang_response = self.download(self.LANG_QUERY_URL.format(set_code, first_card_number))

		if not lang_response:
			LOGGER.error(f"Failed to get set printing languages for {set_code} due to bad response: {lang_response}")
			return []

		set_languages = {LANGUAGE_MAP.get(card.get("lang")) for card in lang_response.get("data", [])}

		return sorted(filter(None, set_languages))
