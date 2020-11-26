"""Test the CardMarket provider."""

import pytest_mock
import pytest

from mtgjson5.arg_parser import parse_args
from mtgjson5.providers.cardmarket import CardMarketProvider
from typing import Dict
from singleton_decorator import singleton

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
    pytest.param(
        False,
        {
            "throne of eldraine": {"mcmId": 1111},
            "throne of eldraine: extras": {"mcmId": 2222},
            "throne of eldraine: promos": {"mcmId": 3333},
        },
        None,
        id="Extras set available but no mcm keys",
    ),
]


@pytest.mark.parametrize("keys_found,set_map,expected", testdata)
def test_get_extras_set_id(keys_found, set_map, expected, mocker):
    """Test if it finds sets with ": extras"."""
    # provider = ProviderMock()
    obj = mocker.MagicMock()
    # Trick name mangling which will convert "__keys_found" to "_CardMarketProvider__keys_found"
    obj._CardMarketProvider__keys_found = keys_found
    obj.set_map = set_map
    actual = CardMarketProvider.__wrapped__.get_extras_set_id(obj, "throne of eldraine")
    assert actual == expected
