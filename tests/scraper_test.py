import asyncio

import aiohttp
import pytest
import vcr

from mtgjson4 import builder


@pytest.mark.asyncio
@vcr.use_cassette
async def test_w17(event_loop: asyncio.AbstractEventLoop) -> None:
    W17 = builder.determine_gatherer_sets({'sets': ['W17'], 'all_sets': False})
    b = builder.MtgJson(W17, loop=event_loop)
    json = await b.build_set(W17[0], 'en')
    assert json["block"] == 'Amonkhet'
    assert json["border"] == 'black'
    assert len(json['cards']) == 30
    assert json['code'] == 'W17'
