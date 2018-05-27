import asyncio
import itertools
from typing import Any, AsyncGenerator, Dict, List, Tuple, Union

import aiohttp
import bs4

search_url = 'http://gatherer.wizards.com/Pages/Search/Default.aspx'
main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
legal_url = 'http://gatherer.wizards.com/Pages/Card/Printings.aspx'
foreign_url = 'http://gatherer.wizards.com/Pages/Card/Languages.aspx'


async def ensure_content_downloaded(session: aiohttp.ClientSession, url_to_download: str, max_retries: int = 3, **kwargs: Any) -> str:
    # Ensure we can read the URL and its contents
    for retry in itertools.count():
        try:
            async with session.get(url_to_download, **kwargs) as response:
                return await response.text()
        except aiohttp.ClientError:
            if retry == max_retries:
                raise
            await asyncio.sleep(2)
    raise ValueError


async def get_card_details(session: aiohttp.ClientSession, card_mid: int, printed: bool = False) -> str:
    return await ensure_content_downloaded(session, main_url, params=get_params(card_mid, printed))


async def get_card_legalities(session: aiohttp.ClientSession, card_mid: int) -> str:
    return await ensure_content_downloaded(session, legal_url, params=get_params(card_mid))


async def get_card_foreign_details(session: aiohttp.ClientSession, card_mid: int) -> str:
    return await ensure_content_downloaded(session, foreign_url, params=get_params(card_mid))


def get_params(card_mid: int, printed: bool = False) -> Dict[str, Union[str, int]]:
    return {
        'multiverseid': card_mid,
        'printed': str(printed).lower(),
        'page': 0
    }

SetUrls = List[Tuple[str, Dict[str, Union[str, int]]]]

async def get_checklist_urls(session: aiohttp.ClientSession, set_name: List[str]) -> SetUrls:
    def page_count_for_set(html_data: str) -> int:
        try:
            # Get the last instance of 'pagingcontrols' and get the page
            # number from the URL it contains
            soup_oracle = bs4.BeautifulSoup(html_data, 'html.parser')
            soup_oracle = soup_oracle.select('div[class^=pagingcontrols]')[-1]
            soup_oracle = soup_oracle.findAll('a')

            # If it sees '1,2,3>' will take the '3' instead of '>'
            if '&gt;' in str(soup_oracle[-1]):
                soup_oracle = soup_oracle[-2]
            else:
                soup_oracle = soup_oracle[-1]

            num_page_links = int(str(soup_oracle).split('page=')[1].split('&')[0])
        except IndexError:
            num_page_links = 0

        return num_page_links + 1

    def url_params_for_page(page_number: int) -> Dict[str, Union[str, int]]:
        return {
            'output': 'checklist',
            'sort': 'cn+',
            'action': 'advanced',
            'special': 'true',
            'set': f'["{set_name[0]}"]',
            'page': page_number
        }

    async with session.get(search_url, params=(url_params_for_page(0))) as response:
        first_page = await response.text()

    return [
        (search_url, url_params_for_page(page_number))
        for page_number in range(page_count_for_set(first_page))
    ]


async def generate_mids_by_set(session: aiohttp.ClientSession, set_urls: SetUrls) -> AsyncGenerator[int, None]:
    for url, params in set_urls:
        async with session.get(url, params=params) as response:
            soup_oracle = bs4.BeautifulSoup(await response.text(), 'html.parser')

            # All cards on the page
            for card_info in soup_oracle.findAll('a', {'class': 'nameLink'}):
                yield int(str(card_info).split('multiverseid=')[1].split('"')[0])
