import asyncio
import itertools
import os
from typing import Any, Dict, List, Set, Tuple, Union

import aiohttp
import bs4

import mtgjson4.mtg_storage

SEARCH_URL = 'http://gatherer.wizards.com/Pages/Search/Default.aspx'
MAIN_URL = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
LEGAL_URL = 'http://gatherer.wizards.com/Pages/Card/Printings.aspx'
FOREIGN_URL = 'http://gatherer.wizards.com/Pages/Card/Languages.aspx'
TOKEN_URL = 'https://raw.githubusercontent.com/cockatrice/Magic-Token/master/tokens.xml'

ParamsType = Dict[str, Union[str, int]]
SetUrlsType = List[Tuple[str, ParamsType]]


async def ensure_content_downloaded(session: aiohttp.ClientSession,
                                    url_to_download: str,
                                    max_retries: int = 10,
                                    **kwargs: Any) -> str:
    """
    Sometimes downloads fail. This method will retry up to max_retries to ensure
    we get the data we have requested. Raises error on failure
    """
    # Ensure we can read the URL and its contents
    for retry in range(0, max_retries):
        try:
            async with session.get(url_to_download, **kwargs) as response:
                text = await response.text()  # type: str
                return text
        except aiohttp.ClientError as e:
            print("Failed to download", url_to_download, kwargs, "retry #", retry, e)
            await asyncio.sleep(2)
    raise ValueError


async def get_card_details(session: aiohttp.ClientSession, card_mid: int, printed: bool = False) -> str:
    """
    Download the main page for a specific card_mid
    """
    return await ensure_content_downloaded(session, MAIN_URL, params=get_params(card_mid, printed))


async def get_card_legalities(session: aiohttp.ClientSession, card_mid: int) -> str:
    """
    Download the legal page for a specific card_mid
    """
    return await ensure_content_downloaded(session, LEGAL_URL, params=get_params(card_mid))


async def get_card_foreign_details(session: aiohttp.ClientSession, card_mid: int) -> str:
    """
    Download the foreign prints page for a specific card_mid
    """
    return await ensure_content_downloaded(session, FOREIGN_URL, params=get_params(card_mid))


async def get_all_tokens(session: aiohttp.ClientSession) -> str:
    """
    Download the Tokens XML from Magic-Token
    This will be parsed in another method
    """
    return await ensure_content_downloaded(session, TOKEN_URL)


def get_params(card_mid: int, printed: bool = False) -> ParamsType:
    """
    Get the parameters necessary for URL manipulations to get the card's data
    """
    return {'multiverseid': card_mid, 'printed': str(printed).lower(), 'page': 0}


async def get_checklist_urls(session: aiohttp.ClientSession, set_name: List[str]) -> SetUrlsType:
    """
    Function will get all URLs needed by other methods to download the card data.
    We will give back the URLs for the pages, which can be parsed further for
    each individual card on the page.
    """

    def page_count_for_set(html_data: str) -> int:
        """
        Determine how many pages of card data exist for a set. The way Gatherer does
        it with the "1, 2, 3 >" we need to pull the last numerical value to get the
        real answer.
        """
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
        """
        Get the parameters necessary for determing how many pages exist for the set.
        """
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


async def generate_mids_by_set(session: aiohttp.ClientSession, set_urls: SetUrlsType, set_name: str) -> List[int]:
    """
    Function will take download all content from the set_urls and parse them to determine
    all the card's needed for the set. Will take the MIDs and return them back for
    further downloads of each individual card
    """

    # Cache Read
    if os.path.exists(os.path.join(mtgjson4.mtg_storage.CACHE_DIR, 'set_mids', set_name + '.txt')):
        with mtgjson4.mtg_storage.open_cache_location(f'set_mids/{set_name}.txt', 'r') as f:
            return eval(f.read())

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

    # Cache Write
    with mtgjson4.mtg_storage.open_cache_location(f'set_mids/{set_name}.txt', 'w') as f:
        f.write(str(card_mids_to_parse))

    return card_mids_to_parse
