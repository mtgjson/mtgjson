import asyncio
from mtgjson4 import mtg_builder
import pytest
import vcr


@pytest.mark.asyncio
@vcr.use_cassette
async def test_w17(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Very basic test.  Can the scraper scrape Gatherer?
    """
    w17 = mtg_builder.determine_gatherer_sets({'sets': ['W17'], 'all_sets': False})
    b = mtg_builder.MTGJSON(w17, loop=event_loop)
    json = await b.build_set(w17[0], 'en')
    assert json["block"] == 'Amonkhet'
    assert json["border"] == 'black'
    assert len(json['cards']) == 30
    assert json['code'] == 'W17'


@pytest.mark.asyncio
@vcr.use_cassette
async def test_isd(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Scrape Innistrad.  Make sure those DFCs work.
    """
    isd = mtg_builder.determine_gatherer_sets({'sets': ['ISD'], 'all_sets': False})
    b = mtg_builder.MTGJSON(isd, loop=event_loop)
    json = await b.build_set(isd[0], 'en')
    delver = [c for c in json['cards'] if c['multiverseid'] == 226749][0]
    assert delver['name'] == 'Delver of Secrets'
    assert delver['names'] == ["Delver of Secrets", "Insectile Aberration"]
    assert delver["number"] == "51a"
    aberration = [c for c in json['cards'] if c['multiverseid'] == 226755][0]
    assert aberration['name'] == 'Insectile Aberration'
    assert aberration['number'] == '51b'
