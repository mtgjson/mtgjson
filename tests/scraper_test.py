import asyncio

import pytest
import vcr

from mtgjson4 import mtg_builder, mtg_storage

mtg_storage.ensure_set_dir_exists()

TEST_VCR = vcr.VCR(
    record_mode='new_episodes',
    path_transformer=vcr.VCR.ensure_suffix('.yaml'),
)


@pytest.mark.asyncio
@TEST_VCR.use_cassette
async def test_w17(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Very basic test.  Can the scraper scrape Gatherer?
    """
    w17 = mtg_builder.determine_gatherer_sets({'sets': ['W17'], 'all_sets': False})
    builder = mtg_builder.MTGJSON(w17, loop=event_loop)
    json = await builder.build_set(w17[0], 'en')
    assert json["block"] == 'Amonkhet'
    assert json["border"] == 'black'
    assert len(json['cards']) == 30
    assert json['code'] == 'W17'

@pytest.mark.dfc
@pytest.mark.asyncio
@TEST_VCR.use_cassette
async def test_isd(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Scrape Innistrad.  Make sure those DFCs work.
    """
    isd = mtg_builder.determine_gatherer_sets({'sets': ['ISD'], 'all_sets': False})
    builder = mtg_builder.MTGJSON(isd, loop=event_loop)
    json = await builder.build_set(isd[0], 'en')
    assert len(json['cards']) == 284
    delver = [c for c in json['cards'] if c['multiverseid'] == 226749][0]
    assert delver['name'] == 'Delver of Secrets'
    assert delver['names'] == ["Delver of Secrets", "Insectile Aberration"]
    assert delver["number"] == "51a"
    assert delver['layout'] == 'Double-Faced'
    aberration = [c for c in json['cards'] if c['multiverseid'] == 226755][0]
    assert aberration['name'] == 'Insectile Aberration'
    assert aberration['names'] == ["Delver of Secrets", "Insectile Aberration"]
    assert aberration['number'] == '51b'
    assert aberration['layout'] == 'Double-Faced'


@pytest.mark.flip
@pytest.mark.asyncio
@TEST_VCR.use_cassette
async def test_chk(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Scrape Champions of Kamigawa.  Make sure those Flip Cards work.
    """
    chk = mtg_builder.determine_gatherer_sets({'sets': ['CHK'], 'all_sets': False})
    builder = mtg_builder.MTGJSON(chk, loop=event_loop)
    json = await builder.build_set(chk[0], 'en')
    lavarunner = [c for c in json['cards'] if c['multiverseid'] == 78694][0]
    assert lavarunner['name'] == 'Akki Lavarunner'
    assert lavarunner['names'] == ["Akki Lavarunner", "Tok-Tok, Volcano Born"]
    assert lavarunner["number"] == "153a"
    toktok = [c for c in json['cards'] if c['multiverseid'] == 78694][1]
    assert toktok['name'] == 'Tok-Tok, Volcano Born'
    assert toktok['names'] == ["Akki Lavarunner", "Tok-Tok, Volcano Born"]
    assert toktok["number"] == "153b"

@pytest.mark.split
@pytest.mark.asyncio
@TEST_VCR.use_cassette
async def test_inv(event_loop: asyncio.AbstractEventLoop) -> None:
    """
    Scrape Invasion.  Make sure those Split Cards work.
    """
    inv = mtg_builder.determine_gatherer_sets({'sets': ['INV'], 'all_sets': False})
    builder = mtg_builder.MTGJSON(inv, loop=event_loop)
    json = await builder.build_set(inv[0], 'en')
    stand = [c for c in json['cards'] if c['multiverseid'] == 20573][0]
    assert stand['name'] == 'Stand'
    assert stand['names'] == ["Stand", "Deliver"]
    assert stand["number"] == "292a"
    assert stand["layout"] == 'split'
    deliver = [c for c in json['cards'] if c['multiverseid'] == 20573][1]
    assert deliver['name'] == 'Deliver'
    assert deliver['names'] == ["Stand", "Deliver"]
    assert deliver["number"] == "292b"
    assert deliver["layout"] == 'split'
