# test_myclient.py
import responses
import pytest
import requests_cache
import json
from pathlib import Path

from mtgjson5.providers.scryfall.monolith import ScryfallProvider
import mtgjson5.set_builder as set_builder

@pytest.fixture
def patched_session(tmp_path, monkeypatch):
    """
    Force the singleton to use _our_ CachedSession with a per-test cache.
    """
    sess = requests_cache.CachedSession(
        cache_name=str(tmp_path / "cache.sqlite"),
        backend="sqlite",
        expire_after=3600,  # 1 hour
        allowable_methods=("GET"),
        stale_if_error=True,
    )

    def _factory(*args, **kwargs):
        # Ignore args/kwargs from prod code and return our controlled session
        return sess

    # patch ScryfallProvider usage of requests_cache
    monkeypatch.setattr(ScryfallProvider, "session", sess, raising=False)
    return sess

@pytest.fixture(autouse=True)
def reset_singleton():
    """
    Make sure each test gets a fresh singleton.
    """
    try:
        ScryfallProvider._instance = None
    except Exception:
        pass
    yield
    try:
        ScryfallProvider._instance = None
    except Exception:
        pass

@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
    """Fixture to execute asserts before and after a test is run"""
    # Setup
    #mock the scryfall hit on singleton instantiation
    cards_without_limits_url = "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)%20or%20(o:deck%20o:have%20o:up%20o:to%20o:cards%20o:named)"
    
    # TODO: Replace this with a proper mocked response.
    cards_without_limits_mocked_json = {"ok": True, "x": 1}

    # mock the download url response
    responses.add(
        responses.GET,
        cards_without_limits_url,
        json=cards_without_limits_mocked_json,
        status=200,
        content_type="application/json",
    )

    client = ScryfallProvider()

    yield # this is where the testing happens

    # Teardown
    # add anything here to tear down test

@responses.activate
def test_get_scryfall_set_data_returns_expected_data(patched_session):
    # Arrange
    download_url = "https://api.scryfall.com/sets/TSP"

    # Get the parent directory of the current script
    CUR_DIR = Path(__file__).parent.absolute()

    # Define the relative path to the JSON file
    json_path = CUR_DIR.joinpath("mocked_data","TSP_SET_DATA.json")
    with open(json_path, 'r') as file_content:
        download_mocked_json = json.load(file_content)

    # mock the download response
    responses.add(responses.GET, download_url, json=download_mocked_json, status=200)

    # Act
    output = set_builder.get_scryfall_set_data("TSP")
    
    # Assert
    assert output == download_mocked_json

@responses.activate
def test_get_scryfall_set_data_handles_errors(patched_session):
    # Arrange
    download_url = "https://api.scryfall.com/sets/BAD_SET_CODE"

    # mocked error response
    mocked_error_response = {
        "object": "error",
        "code": "not_found",
        "status": 404,
        "details": "No Magic set found for the given code or ID"
    }

    # mock the download response
    responses.add(responses.GET, download_url, json=mocked_error_response, status=200)

    # Act
    output = set_builder.get_scryfall_set_data("BAD_SET_CODE")
    
    # Assert
    assert output == None
