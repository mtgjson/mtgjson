"""Test the CardMarket provider."""

import pytest

from mtgjson5.providers.cardmarket.monolith import CardMarketProvider


testdata = [
	pytest.param(
		True,
		{
			"throne of eldraine": {"mcmId": 1111},
			"throne of eldraine: extras": {"mcmId": 2222},
			"throne of eldraine: promos": {"mcmId": 3333},
		},
		2222,
		id="Extras set available",
	),
	pytest.param(
		True,
		{
			"throne of eldraine": {"mcmId": 1111},
			"throne of eldraine: promos": {"mcmId": 3333},
		},
		None,
		id="Extras set not available",
	),
]


@pytest.mark.parametrize("keys_found,set_map,expected", testdata)
def test_get_extras_set_id(keys_found, set_map, expected, mocker):
	"""Test if it finds sets with ": extras"."""
	obj = mocker.MagicMock()
	obj.set_map = set_map
	actual = CardMarketProvider.__wrapped__.get_extras_set_id(obj, "throne of eldraine")
	assert actual == expected
