#!/usr/bin/env python3

import aiohttp
import asyncio
import bs4
import contextlib
import itertools
import json
import pathlib
import re
import time

import mtgjson4.shared_info


OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'outputs'


async def ensure_content_downloaded(session, url_to_download, max_retries=3, **kwargs):
    # Ensure we can read the URL and its contents
    for retry in itertools.count():
        try:
            async with session.get(url_to_download, **kwargs) as response:
                return await response.text()
        except aiohttp.ClientError:
            if retry == max_retries:
                raise
            await asyncio.sleep(2)


async def get_checklist_urls(session, set_name):
    def page_count_for_set(html_data):
        try:
            # Get the last instance of 'pagingcontrols' and get the page
            # number from the URL it contains
            soup = bs4.BeautifulSoup(html_data, 'html.parser')
            soup = soup.select('div[class^=pagingcontrols]')[-1]
            soup = soup.findAll('a')

            # If it sees '1,2,3>' will take the '3' instead of '>'
            if '&gt;' in str(soup[-1]):
                soup = soup[-2]
            else:
                soup = soup[-1]

            num_page_links = int(str(soup).split('page=')[1].split('&')[0])
        except IndexError:
            num_page_links = 0

        return num_page_links + 1

    def url_params_for_page(page_number):
        return {
            'output': 'checklist',
            'sort': 'cn+',
            'action': 'advanced',
            'special': 'true',
            'set': f'["{set_name}"]',
            'page': page_number
        }

    main_url = 'http://gatherer.wizards.com/Pages/Search/Default.aspx'
    main_params = url_params_for_page(0)

    async with session.get(main_url, params=main_params) as response:
        html = await response.text()

    return [
        (main_url, url_params_for_page(page_number))
        for page_number in range(page_count_for_set(html))
    ]


async def generate_mids_by_set(session, set_urls):
    for url, params in set_urls:
        async with session.get(url, params=params) as response:
            soup = bs4.BeautifulSoup(await response.text(), 'html.parser')

            # All cards on the page
            for card_info in soup.findAll('a', {'class': 'nameLink'}):
                yield str(card_info).split('multiverseid=')[1].split('"')[0]


async def download_cards_by_mid_list(session, set_name, multiverse_ids, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
    legal_url = 'http://gatherer.wizards.com/Pages/Card/Printings.aspx'
    foreign_url = 'http://gatherer.wizards.com/Pages/Card/Languages.aspx'

    async def build_main_part(card_mid, card_info, second_card=False):
        html = await ensure_content_downloaded(session, main_url, params=get_url_params(card_mid))

        # Parse web page so we can gather all data from it
        soup = bs4.BeautifulSoup(html, 'html.parser')
        # Get Card Multiverse ID
        card_info['multiverseid'] = int(card_mid)

        # Determine if Card is Normal, Flip, or Split
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'
        cards_total = len(soup.select('table[class^=cardDetails]'))
        if cards_total == 1:
            card_layout = 'normal'
        elif cards_total == 2:
            card_layout = 'double'
            if second_card:
                div_name = div_name[:-3] + '_ctl03_{}'
            else:
                div_name = div_name[:-3] + '_ctl02_{}'
                additional_cards.append(loop.create_task(build_card(card_mid, second_card=True)))
        else:
            card_layout = 'unknown'

        # Get Card Name
        name_row = soup.find(id=div_name.format('nameRow'))
        name_row = name_row.findAll('div')[-1]
        card_name = name_row.get_text(strip=True)
        card_info['name'] = card_name

        # Get other side's name for the user
        if card_layout == 'double':
            if 'ctl02' in div_name:
                other_div_name = div_name.replace('02', '03')
            else:
                other_div_name = div_name.replace('03', '02')
            other_name_row = soup.find(id=other_div_name.format('nameRow'))
            other_name_row = other_name_row.findAll('div')[-1]
            card_other_name = other_name_row.get_text(strip=True)
            card_info['names'] = [card_name, card_other_name]

        # Get Card CMC
        cmc_row = soup.find(id=div_name.format('cmcRow'))
        if cmc_row is None:
            card_info['cmc'] = 0
        else:
            cmc_row = cmc_row.findAll('div')[-1]
            card_cmc = cmc_row.get_text(strip=True)
            card_info['cmc'] = int(card_cmc)

        # Get Card Colors, Cost, and Color Identity (start)
        card_color_identity = set()
        mana_row = soup.find(id=div_name.format('manaRow'))
        if mana_row is None:
            card_info['colors'] = []
            card_info['manaCost'] = ''
        else:
            mana_row = mana_row.findAll('div')[-1]
            mana_row = mana_row.findAll('img')

            card_colors = set()
            card_cost = ''

            for symbol in mana_row:
                symbol_value = symbol['alt']
                symbol_mapped = mtgjson4.shared_info.get_symbol_short_name(symbol_value)
                card_cost += f'{{{symbol_mapped}}}'
                if symbol_mapped in mtgjson4.shared_info.COLORS:
                    card_color_identity.add(symbol_mapped)
                    card_colors.add(symbol_mapped)

            # Remove duplicates and sort in WUBRG order
            # TODO use canonical color order
            card_info['colors'] = list(filter(lambda c: c in card_colors, mtgjson4.shared_info.COLORS))
            card_info['manaCost'] = card_cost

        # Get Card Type(s)
        card_super_types = []
        card_types = []
        type_row = soup.find(id=div_name.format('typeRow'))
        type_row = type_row.findAll('div')[-1]
        type_row = type_row.get_text(strip=True).replace('  ', ' ')

        if '—' in type_row:
            supertypes_and_types, subtypes = type_row.split('—')
        else:
            supertypes_and_types = type_row
            subtypes = ''

        for value in supertypes_and_types.split():
            if value in mtgjson4.shared_info.SUPERTYPES:
                card_super_types.append(value)
            elif value in mtgjson4.shared_info.CARD_TYPES:
                card_types.append(value)
            else:
                raise ValueError(f'Unknown supertype or card type: {value}')

        card_sub_types = subtypes.split()

        card_info['supertypes'] = card_super_types
        card_info['types'] = card_types
        card_info['subtypes'] = card_sub_types
        card_info['type'] = type_row

        # Get Card Text and Color Identity (remaining)
        text_row = soup.find(id=div_name.format('textRow'))
        if text_row is None:
            card_info['text'] = ''
        else:
            text_row = text_row.select('div[class^=cardtextbox]')

            card_text = ''
            for div in text_row:
                # Start by replacing all images with alternative text
                images = div.findAll('img')
                for symbol in images:
                    symbol_value = symbol['alt']
                    symbol_mapped = mtgjson4.shared_info.get_symbol_short_name(symbol_value)
                    symbol.replace_with(f'{{{symbol_mapped}}}')
                    if symbol_mapped in mtgjson4.shared_info.COLORS:
                        card_color_identity.add(symbol_mapped)

                # Next, just add the card text, line by line
                card_text += div.get_text() + '\n'

            card_info['text'] = card_text[:-1]  # Remove last '\n'

        # Remove duplicates and sort in WUBRG order
        # TODO use canonical color order
        card_info['colorIdentity'] = list(filter(lambda c: c in card_color_identity, mtgjson4.shared_info.COLORS))

        # Get Card Flavor Text
        flavor_row = soup.find(id=div_name.format('flavorRow'))
        if flavor_row is not None:
            flavor_row = flavor_row.select('div[class^=flavortextbox]')

            card_flavor_text = ''
            for div in flavor_row:
                card_flavor_text += div.get_text() + '\n'

            card_info['flavor'] = card_flavor_text[:-1]  # Remove last '\n'

        # Get Card P/T OR Loyalty OR Hand/Life
        pt_row = soup.find(id=div_name.format('ptRow'))
        if pt_row is not None:
            pt_row = pt_row.findAll('div')[-1]
            pt_row = pt_row.get_text(strip=True)

            # If Vanguard
            if 'Hand Modifier' in pt_row:
                pt_row = pt_row.split('\xa0,\xa0')
                card_hand_mod = pt_row[0].split(' ')[-1]
                card_life_mod = pt_row[1].split(' ')[-1][:-1]

                card_info['hand'] = card_hand_mod
                card_info['life'] = card_life_mod
            elif '/' in pt_row:
                card_power, card_toughness = pt_row.split('/')
                card_info['power'] = card_power.strip()
                card_info['toughness'] = card_toughness.strip()
            else:
                card_info['loyalty'] = pt_row.strip()

        # Get Card Rarity
        rarity_row = soup.find(id=div_name.format('rarityRow'))
        rarity_row = rarity_row.findAll('div')[-1]
        card_rarity = rarity_row.find('span').get_text(strip=True)
        card_info['rarity'] = card_rarity

        # Get Card Set Number
        number_row = soup.find(id=div_name.format('numberRow'))
        if number_row is not None:
            number_row = number_row.findAll('div')[-1]
            card_number = number_row.get_text(strip=True)
            card_info['number'] = card_number

        # Get Card Artist
        artist_row = soup.find(id=div_name.format('artistRow'))
        artist_row = artist_row.findAll('div')[-1]
        card_artist = artist_row.find('a').get_text(strip=True)
        card_info['artist'] = card_artist

        # Get Card Watermark
        watermark_row = soup.find(id=div_name.format('markRow'))
        if watermark_row is not None:
            watermark_row = watermark_row.findAll('div')[-1]
            card_watermark = watermark_row.get_text(strip=True)
            card_info['watermark'] = card_watermark

        # Get Card Reserve List Status
        if card_info['name'] in mtgjson4.shared_info.RESERVE_LIST:
            card_info['reserved'] = True

        # Get Card Rulings
        rulings_row = soup.find(id=div_name.format('rulingsRow'))
        if rulings_row is not None:
            rulings_dates = rulings_row.findAll('td', id=re.compile(r'\w*_rulingDate\b'))
            rulings_text = rulings_row.findAll('td', id=re.compile(r'\w*_rulingText\b'))
            card_info['rulings'] = [
                {
                    'date': ruling_date.get_text(),
                    'text': ruling_text.get_text()
                }
                for ruling_date, ruling_text in zip(rulings_dates, rulings_text)
            ]

        # Get Card Sets
        card_info['printings'] = [set_name]
        sets_row = soup.find(id=div_name.format('otherSetsRow'))
        if sets_row is not None:
            images = sets_row.findAll('img')
            card_info['printings'] += [
                symbol['alt'].split('(')[0].strip()
                for symbol in images
            ]

    async def build_legalities_part(card_mid, card_info):
        try:
            html = await ensure_content_downloaded(session, legal_url, params=get_url_params(card_mid))
        except aiohttp.ClientError:
            # if Gatherer errors, omit the data for now
            # TODO remove this and handle Gatherer errors on a case-by-case basis
            return

        # Parse web page so we can gather all data from it
        soup = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Legalities
        format_rows = soup.select('table[class^=cardList]')[1]
        format_rows = format_rows.select('tr[class^=cardItem]')
        card_formats = []
        with contextlib.suppress(IndexError):  # if no legalities, only one tr with only one td
            for div in format_rows:
                table_rows = div.findAll('td')
                card_format_name = table_rows[0].get_text(strip=True)
                card_format_legal = table_rows[1].get_text(strip=True)  # raises IndexError if no legalities

                card_formats.append({
                    'format': card_format_name,
                    'legality': card_format_legal
                })

            card_info['legalities'] = card_formats

    async def build_foreign_part(card_mid, card_info):
        try:
            html = await ensure_content_downloaded(session, foreign_url, params=get_url_params(card_mid))
        except aiohttp.ClientError:
            # if Gatherer errors, omit the data for now
            # TODO remove this and handle Gatherer errors on a case-by-case basis
            return

        # Parse web page so we can gather all data from it
        soup = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Foreign Information
        language_rows = soup.select('table[class^=cardList]')[0]
        language_rows = language_rows.select('tr[class^=cardItem]')

        card_languages = []
        for div in language_rows:
            table_rows = div.findAll('td')

            a_tag = table_rows[0].find('a')
            foreign_mid = a_tag['href'].split('=')[-1]
            card_language_mid = foreign_mid
            card_foreign_name_in_language = a_tag.get_text(strip=True)

            card_language_name = table_rows[1].get_text(strip=True)

            card_languages.append({
                'language': card_language_name,
                'name': card_foreign_name_in_language,
                'multiverseid': card_language_mid
            })

        card_info['foreignNames'] = card_languages

    # TODO: Missing types
    # id - Will create myself
    # layout - has to be added after card is created
    # border - Only done if they don't match set (need set config)
    # timeshifted - Only for timeshifted sets (need set config)
    # starter - in starter deck (need set config)
    # mciNumber - gonna have to look it up
    # scryfallNumber - I want to add this
    # variations - Added after the fact

    async def build_card(card_mid, second_card=False):
        card_info = {}

        await build_main_part(card_mid, card_info, second_card=second_card)
        await build_legalities_part(card_mid, card_info)
        await build_foreign_part(card_mid, card_info)

        print('Adding {0} to {1}'.format(card_info['name'], set_name))
        return card_info

    def add_layouts(cards):
        for card_info in cards:
            if 'names' in card_info:
                sides = len(card_info['names'])
            else:
                sides = 1

            if sides == 1:
                if 'hand' in card_info:
                    card_layout = 'Vanguard'
                elif 'Scheme' in card_info['types']:
                    card_layout = 'Scheme'
                elif 'Plane' in card_info['types']:
                    card_layout = 'Plane'
                elif 'Phenomenon' in card_info['types']:
                    card_layout = 'Phenomenon'
                else:
                    card_layout = 'Normal'
            elif sides == 2:
                if 'transform' in card_info['text']:
                    card_layout = 'Double-Faced'
                elif 'aftermath' in card_info['text']:
                    card_layout = 'Aftermath'
                elif 'flip' in card_info['text']:
                    card_layout = 'Flip'
                elif 'split' in card_info['text']:
                    card_layout = 'Split'
                elif 'meld' in card_info['text']:
                    card_layout = 'Meld'
                else:
                    card_2_name = next(card2 for card2 in card_info['names'] if card_info['name'] != card2)
                    card_2_info = next(card2 for card2 in cards if card2['name'] == card_2_name)

                    if 'flip' in card_2_info['text']:
                        card_layout = 'Flip'
                    elif 'transform' in card_2_info['text']:
                        card_layout = 'Double-Faced'
                    else:
                        card_layout = 'Unknown'
            else:
                card_layout = 'Meld'

            card_info['layout'] = card_layout

    def get_url_params(card_mid):
        return {
            'multiverseid': card_mid,
            'printed': 'false',
            'page': 0
        }

    # start asyncio tasks for building each card
    futures = [
        loop.create_task(build_card(card_mid))
        for card_mid in multiverse_ids
    ]

    additional_cards = []

    # then wait until all of them are completed
    await asyncio.wait(futures)
    cards_in_set = []
    for future in futures:
        card = future.result()
        cards_in_set.append(card)

    await asyncio.wait(additional_cards)
    for future in additional_cards:
        card = future.result()
        cards_in_set.append(card)

    add_layouts(cards_in_set)
    return cards_in_set


async def build_set(session, set_name):
    print('BuildSet: Building Set {}'.format(set_name))

    urls_for_set = await get_checklist_urls(session, set_name)
    print('BuildSet: URLs for {0}: {1}'.format(set_name, urls_for_set))

    # mids_for_set = [mid async for mid in generate_mids_by_set(session, urls_for_set)]
    mids_for_set = [439335, 442051, 435172, 182290, 435173]  # DEBUG
    print('BuildSet: MIDs for {0}: {1}'.format(set_name, mids_for_set))

    cards_holder = await download_cards_by_mid_list(session, set_name, mids_for_set)
    print('BuildSet: JSON generated for {}'.format(set_name))

    with (OUTPUT_DIR / '{}.json'.format(set_name)).open('w') as fp:
        json.dump(cards_holder, fp, indent=4, sort_keys=True)
    print('BuildSet: JSON written for {}'.format(set_name))


async def main(loop, session):
    OUTPUT_DIR.mkdir(exist_ok=True)  # make sure outputs dir exists

    async with session:
        # start asyncio tasks for building each set
        futures = [
            loop.create_task(build_set(session, set_name))
            for set_name in mtgjson4.shared_info.GATHERER_SETS
        ]
        # then wait until all of them are completed
        await asyncio.wait(futures)


if __name__ == '__main__':
    start_time = time.time()

    card_loop = asyncio.get_event_loop()
    card_session = aiohttp.ClientSession(loop=card_loop, raise_for_status=True)
    card_loop.run_until_complete(main(card_loop, card_session))

    end_time = time.time()
    print('Time: {}'.format(end_time - start_time))
