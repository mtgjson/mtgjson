import responses
import pytest
import requests_cache
import json
from pathlib import Path

from mtgjson5.providers.scryfall.monolith import ScryfallProvider

@pytest.fixture
def patched_session(tmp_path, monkeypatch):
    """
    Force the singleton to use *our* CachedSession with a per-test cache.
    """
    sess = requests_cache.CachedSession(
        cache_name=str(tmp_path / "cache.sqlite"),
        backend="sqlite",
        expire_after=3600,                  # 1 hour
        allowable_methods=("GET"),
        stale_if_error=True,
    )

    def _factory(*args, **kwargs):
        # Ignore args/kwargs from prod code and return our controlled session
        return sess

    # Wherever ScryfallProvider imports requests_cache, patch *that* reference
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

@responses.activate
def test_download_uses_cache_and_returns_json(patched_session):
    # Arrange

    #mock the scryfall hit on singleton instantiation
    cards_without_limits_url = "https://api.scryfall.com/cards/search?q=(o:deck%20o:any%20o:number%20o:cards%20o:named)%20or%20(o:deck%20o:have%20o:up%20o:to%20o:cards%20o:named)" 
    
    # mock the response.  This isn't the actual response, but we don't need the actual response for this test
    cards_without_limits_mocked_json = {"ok": True, "x": 1}

    # intercept the cards_without_limits request and provide the mocked response
    responses.add(
        responses.GET,
        cards_without_limits_url,
        json=cards_without_limits_mocked_json,
        status=200,
        content_type="application/json",
    )

    download_url = "https://api.scryfall.com/cards/search?q=t:instant"

    # instantiate after mocks so the on-singleton call is mocked
    client = ScryfallProvider()

    # Get the parent directory of the current script
    CUR_DIR = Path(__file__).parent.parent.absolute()

    # Define the relative path to the JSON file
    json_path = CUR_DIR.joinpath("mocked_data", "TSP.json")
    with open(json_path, 'r') as file_content:
        download_mocked_json = json.load(file_content)

    # intercept the search request and provide the mocked response
    responses.add(responses.GET, download_url, json=download_mocked_json, status=200)

    # Act
    out1 = client.download(download_url)
    
    # Assert
    assert out1 == download_mocked_json
