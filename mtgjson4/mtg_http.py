import asyncio
import itertools
from typing import Any, Dict, List, Set, Tuple, Union

import aiohttp
import bs4

SEARCH_URL = 'http://gatherer.wizards.com/Pages/Search/Default.aspx'
MAIN_URL = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
LEGAL_URL = 'http://gatherer.wizards.com/Pages/Card/Printings.aspx'
FOREIGN_URL = 'http://gatherer.wizards.com/Pages/Card/Languages.aspx'

ParamsType = Dict[str, Union[str, int]]
SetUrlsType = List[Tuple[str, ParamsType]]


async def ensure_content_downloaded(session: aiohttp.ClientSession,
                                    url_to_download: str,
                                    max_retries: int = 3,
                                    **kwargs: Any) -> str:
    # Ensure we can read the URL and its contents
    for retry in itertools.count():
        try:
            async with session.get(url_to_download, **kwargs) as response:
                text = await response.text()  # type: str
                return text
        except aiohttp.ClientError:
            if retry == max_retries:
                raise
            await asyncio.sleep(2)
    raise ValueError


async def get_card_details(session: aiohttp.ClientSession, card_mid: int, printed: bool = False) -> str:
    return await ensure_content_downloaded(session, MAIN_URL, params=get_params(card_mid, printed))


async def get_card_legalities(session: aiohttp.ClientSession, card_mid: int) -> str:
    return await ensure_content_downloaded(session, LEGAL_URL, params=get_params(card_mid))


async def get_card_foreign_details(session: aiohttp.ClientSession, card_mid: int) -> str:
    return await ensure_content_downloaded(session, FOREIGN_URL, params=get_params(card_mid))


def get_params(card_mid: int, printed: bool = False) -> ParamsType:
    return {'multiverseid': card_mid, 'printed': str(printed).lower(), 'page': 0}


async def get_checklist_urls(session: aiohttp.ClientSession, set_name: List[str]) -> SetUrlsType:
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

    def url_params_for_page(page_number: int) -> ParamsType:
        return {
            'output': 'checklist',
            'sort': 'cn+',
            'action': 'advanced',
            'special': 'true',
            'set': f'["{set_name[0]}"]',
            'page': page_number
        }

    async with session.get(SEARCH_URL, params=(url_params_for_page(0))) as response:
        first_page = await response.text()

    return [(SEARCH_URL, url_params_for_page(page_number)) for page_number in range(page_count_for_set(first_page))]


async def generate_mids_by_set(session: aiohttp.ClientSession, set_urls: SetUrlsType) -> List[int]:
    card_mids_to_parse: List[int] = list()
    for url, params in set_urls:
        async with session.get(url, params=params) as response:
            soup_oracle = bs4.BeautifulSoup(await response.text(), 'html.parser')

            card_id_exist: Set[str] = set()

            for row_info in soup_oracle.find_all('tr', {'class': 'cardItem'}):
                td_row = row_info.find_all('td')
                gatherer_page_id = td_row[0].get_text(strip=True)

                # Some sets don't have card numbers, like Alpha and Beta
                if (gatherer_page_id not in card_id_exist) or not gatherer_page_id:
                    card_id_exist.add(gatherer_page_id)
                    card_mids_to_parse.append(int(td_row[1].find('a')['href'].split('id=')[1].split('"')[0]))

    return card_mids_to_parse
