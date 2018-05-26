import copy

import aiohttp
import argparse
import ast
import asyncio
import bs4
import contextlib
import hashlib
import itertools
import json
import os
import pathlib
import re
import sys
import time

import mtgjson4.globals

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'outputs'
SET_CONFIG_DIR = pathlib.Path(__file__).resolve().parent / 'set_configs'


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

    def url_params_for_page(page_number):
        return {
            'output': 'checklist',
            'sort': 'cn+',
            'action': 'advanced',
            'special': 'true',
            'set': f'["{set_name[0]}"]',
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
            soup_oracle = bs4.BeautifulSoup(await response.text(), 'html.parser')

            # All cards on the page
            for card_info in soup_oracle.findAll('a', {'class': 'nameLink'}):
                yield str(card_info).split('multiverseid=')[1].split('"')[0]


async def download_cards_by_mid_list(session, set_name, multiverse_ids, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx'
    legal_url = 'http://gatherer.wizards.com/Pages/Card/Printings.aspx'
    foreign_url = 'http://gatherer.wizards.com/Pages/Card/Languages.aspx'

    async def build_main_part(card_mid, card_info, second_card=False):
        # Parse web page so we can gather all data from it
        html_oracle = await ensure_content_downloaded(session, main_url, params=get_url_params(card_mid))
        soup_oracle = bs4.BeautifulSoup(html_oracle, 'html.parser')

        # Get Card Multiverse ID
        card_info['multiverseid'] = int(card_mid)

        # Determine if Card is Normal, Flip, or Split
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'
        cards_total = len(soup_oracle.select('table[class^=cardDetails]'))
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
        name_row = soup_oracle.find(id=div_name.format('nameRow'))
        name_row = name_row.findAll('div')[-1]
        card_name = name_row.get_text(strip=True)
        card_info['name'] = card_name

        # Get other side's name for the user
        if card_layout == 'double':
            if 'ctl02' in div_name:
                other_div_name = div_name.replace('02', '03')
            else:
                other_div_name = div_name.replace('03', '02')
            other_name_row = soup_oracle.find(id=other_div_name.format('nameRow'))
            other_name_row = other_name_row.findAll('div')[-1]
            card_other_name = other_name_row.get_text(strip=True)
            card_info['names'] = [card_name, card_other_name]

        # Get Card CMC
        cmc_row = soup_oracle.find(id=div_name.format('cmcRow'))
        if cmc_row is None:
            card_info['cmc'] = 0
        else:
            cmc_row = cmc_row.findAll('div')[-1]
            card_cmc = cmc_row.get_text(strip=True)
            try:
                card_info['cmc'] = int(card_cmc)
            except ValueError:  # Little Girl causes this, for example
                card_info['cmc'] = float(card_cmc)

        # Get Card Colors, Cost, and Color Identity (start)
        card_color_identity = set()
        mana_row = soup_oracle.find(id=div_name.format('manaRow'))
        if mana_row:
            mana_row = mana_row.findAll('div')[-1]
            mana_row = mana_row.findAll('img')

            card_colors = set()
            card_cost = ''

            for symbol in mana_row:
                symbol_value = symbol['alt']
                symbol_mapped = mtgjson4.globals.get_symbol_short_name(symbol_value)
                card_cost += f'{{{symbol_mapped}}}'
                if symbol_mapped in mtgjson4.globals.COLORS:
                    card_color_identity.add(symbol_mapped)
                    card_colors.add(symbol_mapped)

            # Sort field in WUBRG order
            card_colors = sorted(
                list(filter(lambda c: c in card_colors, mtgjson4.globals.COLORS)),
                key=lambda word: [mtgjson4.globals.COLORS.index(c) for c in word]
            )

            if card_colors:
                card_info['colors'] = card_colors
            if card_cost:
                card_info['manaCost'] = card_cost

        # Get Card Type(s)
        card_super_types = []
        card_types = []
        type_row = soup_oracle.find(id=div_name.format('typeRow'))
        type_row = type_row.findAll('div')[-1]
        type_row = type_row.get_text(strip=True).replace('  ', ' ')

        if '—' in type_row:
            supertypes_and_types, subtypes = type_row.split('—')
        else:
            supertypes_and_types = type_row
            subtypes = ''

        for value in supertypes_and_types.split():
            if value in mtgjson4.globals.SUPERTYPES:
                card_super_types.append(value)
            elif value in mtgjson4.globals.CARD_TYPES:
                card_types.append(value)
            else:
                card_types.append(value)
                # raise ValueError(f'Unknown supertype or card type: {value}')

        card_sub_types = subtypes.split()

        if card_super_types:
            card_info['supertypes'] = card_super_types
        if card_types:
            card_info['types'] = card_types
        if card_sub_types:
            card_info['subtypes'] = card_sub_types
        if type_row:
            card_info['type'] = type_row

        # Get Card Text and Color Identity (remaining)
        text_row = soup_oracle.find(id=div_name.format('textRow'))
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
                    symbol_mapped = mtgjson4.globals.get_symbol_short_name(symbol_value)
                    symbol.replace_with(f'{{{symbol_mapped}}}')
                    if symbol_mapped in mtgjson4.globals.COLORS:
                        card_color_identity.add(symbol_mapped)

                # Next, just add the card text, line by line
                card_text += div.get_text() + '\n'

            card_info['text'] = card_text[:-1]  # Remove last '\n'

        # Sort field in WUBRG order
        card_color_identity = sorted(
            list(filter(lambda c: c in card_color_identity, mtgjson4.globals.COLORS)),
            key=lambda word: [mtgjson4.globals.COLORS.index(c) for c in word]
        )

        if card_color_identity:
            card_info['colorIdentity'] = card_color_identity

        # Get Card Flavor Text
        flavor_row = soup_oracle.find(id=div_name.format('flavorRow'))
        if flavor_row is not None:
            flavor_row = flavor_row.select('div[class^=flavortextbox]')

            card_flavor_text = ''
            for div in flavor_row:
                card_flavor_text += div.get_text() + '\n'

            card_info['flavor'] = card_flavor_text[:-1]  # Remove last '\n'

        # Get Card P/T OR Loyalty OR Hand/Life
        pt_row = soup_oracle.find(id=div_name.format('ptRow'))
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
        rarity_row = soup_oracle.find(id=div_name.format('rarityRow'))
        rarity_row = rarity_row.findAll('div')[-1]
        card_rarity = rarity_row.find('span').get_text(strip=True)
        card_info['rarity'] = card_rarity

        # Get Card Set Number
        number_row = soup_oracle.find(id=div_name.format('numberRow'))
        if number_row is not None:
            number_row = number_row.findAll('div')[-1]
            card_number = number_row.get_text(strip=True)
            card_info['number'] = card_number

        # Get Card Artist
        with contextlib.suppress(AttributeError):  # Un-cards might not have an artist!
            artist_row = soup_oracle.find(id=div_name.format('artistRow'))
            artist_row = artist_row.findAll('div')[-1]
            card_artists = artist_row.find('a').get_text(strip=True).split('&')
            card_info['artist'] = card_artists

        # Get Card Watermark
        watermark_row = soup_oracle.find(id=div_name.format('markRow'))
        if watermark_row is not None:
            watermark_row = watermark_row.findAll('div')[-1]
            card_watermark = watermark_row.get_text(strip=True)
            card_info['watermark'] = card_watermark

        # Get Card Reserve List Status
        if card_info['name'] in mtgjson4.globals.RESERVE_LIST:
            card_info['reserved'] = True

        # Get Card Rulings
        rulings_row = soup_oracle.find(id=div_name.format('rulingsRow'))
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
        card_printings = [set_name[1]]
        sets_row = soup_oracle.find(id=div_name.format('otherSetsRow'))
        if sets_row is not None:
            images = sets_row.findAll('img')

            for symbol in images:
                this_set_name = symbol['alt'].split('(')[0].strip()

                card_printings += (
                    set_code[1] for set_code in SETS_TO_BUILD if this_set_name == set_code[0]
                )

        card_info['printings'] = card_printings

        # Get Card Variations
        variations_row = soup_oracle.find(id=div_name.format('variationLinks'))
        if variations_row is not None:
            card_variations = []

            for variations_info in variations_row.findAll('a', {'class': 'variationLink'}):
                card_variations.append(int(variations_info['href'].split('multiverseid=')[1]))

            with contextlib.suppress(ValueError):
                card_variations.remove(card_info['multiverseid'])  # Don't need this card's MID in its variations

            card_info['variations'] = card_variations

    async def build_legalities_part(card_mid, card_info):
        try:
            html = await ensure_content_downloaded(session, legal_url, params=get_url_params(card_mid))
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return  # Page doesn't work, nothing we can do
            else:
                return

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Legalities
        format_rows = soup_oracle.select('table[class^=cardList]')[1]
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
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return  # Page doesn't work, nothing we can do
            else:
                return

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Foreign Information
        language_rows = soup_oracle.select('table[class^=cardList]')[0]
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

    async def build_id_part(card_mid, card_info):
        card_hash = hashlib.sha3_256()
        card_hash.update(set_name[0].encode('utf-8'))
        card_hash.update(str(card_mid).encode('utf-8'))
        card_hash.update(card_info['name'].encode('utf-8'))

        card_info['cardHash'] = card_hash.hexdigest()

    async def build_original_details(card_mid, card_info, second_card=False):
        html_print = await ensure_content_downloaded(session, main_url, params=get_url_params(card_mid, True))
        soup_print = bs4.BeautifulSoup(html_print, 'html.parser')

        # Determine if Card is Normal, Flip, or Split
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'
        cards_total = len(soup_print.select('table[class^=cardDetails]'))
        if cards_total == 2:
            if second_card:
                div_name = div_name[:-3] + '_ctl03_{}'
            else:
                div_name = div_name[:-3] + '_ctl02_{}'

        # Get Card Original Type
        orig_type_row = soup_print.find(id=div_name.format('typeRow'))
        orig_type_row = orig_type_row.findAll('div')[-1]
        orig_type_row = orig_type_row.get_text(strip=True).replace('  ', ' ')
        if orig_type_row:
            card_info['originalType'] = orig_type_row

        # Get Card Original Text
        text_row = soup_print.find(id=div_name.format('textRow'))
        if text_row is None:
            card_info['originalText'] = ''
        else:
            text_row = text_row.select('div[class^=cardtextbox]')

            card_text = ''
            for div in text_row:
                # Start by replacing all images with alternative text
                images = div.findAll('img')
                for symbol in images:
                    symbol_value = symbol['alt']
                    symbol_mapped = mtgjson4.globals.get_symbol_short_name(symbol_value)
                    symbol.replace_with(f'{{{symbol_mapped}}}')

                # Next, just add the card text, line by line
                card_text += div.get_text() + '\n'

            card_info['originalText'] = card_text[:-1]  # Remove last '\n'

    async def build_card(card_mid, second_card=False):
        card_info = {}

        await build_main_part(card_mid, card_info, second_card=second_card)
        await build_original_details(card_mid, card_info, second_card=second_card)
        await build_legalities_part(card_mid, card_info)
        await build_id_part(card_mid, card_info)
        await build_foreign_part(card_mid, card_info)

        print('\tAdding {0} to {1}'.format(card_info['name'], set_name[0]))
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

    def get_url_params(card_mid, is_printed=False):
        return {
            'multiverseid': card_mid,
            'printed': str(is_printed).lower(),
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
        card_future = future.result()
        cards_in_set.append(card_future)

    with contextlib.suppress(ValueError):  # if no double-sided cards, gracefully skip
        await asyncio.wait(additional_cards)
        for future in additional_cards:
            card_future = future.result()
            cards_in_set.append(card_future)

    add_layouts(cards_in_set)

    return cards_in_set


def find_file(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)


def determine_gatherer_sets(args):
    def try_to_append(root_p, file_p):
        with pathlib.Path(root_p, file_p).open('r') as fp:
            this_set_name = json.load(fp)
            if 'SET' in this_set_name:
                all_sets.append([this_set_name['SET']['name'], file.split('.json')[0]])

    all_sets = list()
    if args['all_sets']:
        for root, dirs, files in os.walk(SET_CONFIG_DIR):
            for file in files:
                if file.endswith('.json'):
                    try_to_append(root, file)
    else:
        for root, dirs, files in os.walk(SET_CONFIG_DIR):
            for file in files:
                set_name = file.split('.json')[0]
                if set_name in args['sets']:
                    try_to_append(root, file)
                    args['sets'].remove(set_name)

        # Ensure all sets provided by the user are valid
        if len(args['sets']) > 0:
            print("MTGJSON: Invalid Set Code(s) provided: {}".format(args['sets']))
            exit(1)

    return all_sets


async def apply_set_config_options(set_name, cards_dictionary):
    return_product = dict()

    # Will search the tree of set_configs to find the file
    with (pathlib.Path(find_file("{}.json".format(set_name[1]), SET_CONFIG_DIR))).open('r') as fp:
        file_response = json.load(fp)

        for key, value in file_response['SET'].items():
            return_product[key] = value

        if 'SET_CORRECTIONS' in file_response.keys():
            match_replace_rules = str(file_response['SET_CORRECTIONS'])
            match_replace_rules = ast.literal_eval(match_replace_rules)

            for replacement_rule in match_replace_rules:
                with contextlib.suppress(KeyError):  # If there's no match, it's deprecated
                    replacement_match = replacement_rule['match']

                if 'replace' in replacement_rule.keys():
                    fix_type = 'replace'
                    replacement_update = replacement_rule['replace']
                elif 'fixForeignNames' in replacement_rule.keys():
                    fix_type = 'fixForeignNames'
                    replacement_update = replacement_rule['fixForeignNames']
                else:
                    continue

                for key, value in replacement_match.items():
                    if isinstance(value, list):
                        cards_to_modify = [
                            card
                            for card in cards_dictionary if key in card.keys() and card[key] in value
                        ]
                    elif isinstance(value, str) or isinstance(value, int):
                        cards_to_modify = [
                            card
                            for card in cards_dictionary if key in card.keys() and card[key] == value
                        ]
                    else:
                        continue

                    if fix_type == 'replace':
                        for key_name, replacement in replacement_update.items():
                            for card in cards_to_modify:
                                card[key_name] = replacement
                    elif fix_type == 'fixForeignNames':
                        for lang_replacements in replacement_update:
                            language_name = lang_replacements['language']
                            new_name = lang_replacements['name']

                            for card in cards_to_modify:
                                for foreign_names_field in card['foreignNames']:
                                    if foreign_names_field['language'] == language_name:
                                        foreign_names_field['name'] = new_name

    return_product['cards'] = cards_dictionary

    return return_product


async def build_set(session, set_name, language):
    async def get_mids_for_downloading():
        print('BuildSet: Building Set {}'.format(set_name[0]))

        urls_for_set = await get_checklist_urls(session, set_name)
        print('BuildSet: Acquired URLs for {}'.format(set_name[0]))

        # mids_for_set = [401847, 401889, 401890]
        ids_to_return = [mid async for mid in generate_mids_by_set(session, urls_for_set)]
        return ids_to_return

    async def build_then_print_stuff(mids_for_set, lang=None):
        if lang:
            set_stat = '{0}.{1}'.format(set_name[0], lang)
            set_output = '{0}.{1}'.format(set_name[1], lang)
        else:
            set_stat = str(set_name[0])
            set_output = str(set_name[1])

        print('BuildSet: Determined MIDs for {0}: {1}'.format(set_stat, mids_for_set))

        cards_holder = await download_cards_by_mid_list(session, set_name, mids_for_set)

        print('BuildSet: Applied Set Config options for {}'.format(set_stat))
        json_ready = await apply_set_config_options(set_name, cards_holder)

        print('BuildSet: Generated JSON for {}'.format(set_stat))
        with (OUTPUT_DIR / '{}.json'.format(set_output)).open('w', encoding='utf-8') as fp:
            json.dump(json_ready, fp, indent=4, sort_keys=True, ensure_ascii=False)
            print('BuildSet: JSON written for {0} ({1})'.format(set_stat, set_name[1]))

        return json_ready

    async def build_foreign_language():
        if not os.path.isfile(os.path.join(OUTPUT_DIR, '{}.json'.format(set_name[1]))):
            print('BuildSet: Set {0} not built in English. Do that first before {1}'.format(set_name[1], language))
            return

        with (OUTPUT_DIR / '{}.json'.format(set_name[1])).open('r') as fp:
            json_input = json.load(fp)

        if ('translations' not in json_input.keys()) or (language not in json_input['translations'].keys()):
            print("BuildSet: Cannot translate {0} to {1}. Update set_configs".format(set_name[1], language))
            return

        foreign_mids_for_set = []
        for card in json_input['cards']:
            full_name_lang_to_build = mtgjson4.globals.get_language_long_name(language)
            for lang_dict in card['foreignNames']:
                if lang_dict['language'] == full_name_lang_to_build:
                    foreign_mids_for_set.append(int(lang_dict['multiverseid']))
                    break

        # Write to file the foreign build
        await build_then_print_stuff(foreign_mids_for_set, language)
        return

    if language == 'en':
        await build_then_print_stuff(await get_mids_for_downloading())
    else:
        await build_foreign_language()


async def main(loop, session, language_to_build):
    OUTPUT_DIR.mkdir(exist_ok=True)  # make sure outputs dir exists

    async with session:
        # start asyncio tasks for building each set
        futures = [
            loop.create_task(build_set(session, set_name, language_to_build))
            for set_name in SETS_TO_BUILD
        ]
        # then wait until all of them are completed
        await asyncio.wait(futures)


def create_all_sets_files():
    # Set Variables
    all_sets = dict()
    all_sets_with_extras = dict()
    all_sets_array = list()
    all_sets_array_with_extras = list()

    # Cards Variables
    all_cards = dict()
    all_cards_with_extras = dict()

    # Other Stuff
    previous_seen_set_codes = dict()
    tainted_cards = list()
    ignored_sets = ['UGL', 'UST', 'UNH']

    def ready_to_diff(obj):
        if isinstance(obj, list):
            return ' '.join(obj)

        if isinstance(obj, dict):
            keys = sorted(obj.keys())
            arr = [str({key: ready_to_diff(obj[key])}) for key in keys]
            return ' '.join(arr)

        return obj

    def process_card(card_set, card):
        if card['name'] not in all_cards_with_extras:
            all_cards_with_extras[card['name']] = dict()

        if card['name'] not in previous_seen_set_codes:
            previous_seen_set_codes[card['name']] = dict()

        def check_taint(field_name, field_value=None):
            if card_set['code'] in ignored_sets:
                return

            if field_value is None:
                if field_name in card:
                    field_value = card[field_name]

            if field_name not in all_cards_with_extras[card['name']]:
                return

            previous_value = all_cards_with_extras[card['name']][field_name]

            taint = False
            diff = None

            if previous_value:
                if field_value is None:
                    taint = True
                else:
                    prev = ready_to_diff(previous_value)
                    field = ready_to_diff(field_value)

                    if prev != field:
                        taint = True

            if taint:
                tainted_cards.append({'card': card, 'fieldName': field_name})

        for field_name, field_type in mtgjson4.globals.FIELD_TYPES:
            if field_name in mtgjson4.globals.SET_SPECIFIC_FIELDS:
                continue

            if field_name not in previous_seen_set_codes[card['name']]:
                previous_seen_set_codes[card['name']][field_name] = list()

            field_value = card[field_name]
            if field_name in mtgjson4.globals.ORACLE_FIELDS and field_value != 'foreignNames':
                check_taint(field_name, field_value)

            previous_seen_set_codes[card['name']][field_name].append(card_set['code'])
            all_cards_with_extras[card['name']][field_name] = field_value

        return card

    def process_set(sets):
        for a_set in sets:
            for card in sets['cards']:
                card = process_card(a_set, card)

            a_set.remove('isMCISet')
            a_set.remove('magicRaritiesCode')
            a_set.remove('essentialMagicCode')
            a_set.remove('useMagicRaritiesNumber')

        simple_set = copy.copy(sets)
        for simple_set_card in simple_set['cards']:
            for extra_field in mtgjson4.globals.EXTRA_FIELDS:
                simple_set_card.remove(extra_field)

        return [sets, simple_set]

    sets_in_output = list()
    for file in SET_CONFIG_DIR:
        with file.open('r') as fp:
            file_content = json.load(fp)
            sets_in_output.append(file_content)

    params = {'sets': {}}
    for set_data in sets_in_output:
        params['sets'][set_data['code']] = {
            'code': set_data['code'],
            'releaseDate': set_data['releaseDate']
        }

        full_simple_sets = process_set(sets_in_output)
        
        all_sets_with_extras[set_data['code']] = full_simple_sets[0]
        all_sets_array_with_extras.append(full_simple_sets[0])
        all_sets[set_data['code']] = full_simple_sets[1]
        all_sets_array.append(full_simple_sets[1])

        # Doesn't seem relevant as we're building before
        # full_simple_size = save_set(set_data['code'], full_simple_sets[0], full_simple_sets[1])
        # params['sets'][set_data['code']]['simpleSize'] = full_simple_size[0]
        # params['sets'][set_data['code']]['fullSize'] = full_simple_size[1]


if __name__ == '__main__':
    # Start by processing all arguments to the program
    parser = argparse.ArgumentParser(description=mtgjson4.globals.DESCRIPTION)

    # Can't have both sets and all sets
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--sets', metavar='SET', nargs='+', type=str,
                       help='What set(s) to build (cannot be used with --all-sets)')
    group.add_argument('--all-sets', action='store_true', help='Build all sets (cannot be used with --sets)')
    group.add_argument('-v', '--version', action='store_true', help='MTGJSON version information')

    parser.add_argument('--language', default=['en'], metavar='LANG', type=str, nargs=1,
                        help='Build foreign language version (English must have been built prior)')

    # If user supplies no arguments, show help screen
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        exit(1)

    cl_args = vars(parser.parse_args())
    lang_to_process = cl_args['language'][0]

    # Get version info and exit
    if cl_args['version']:
        print(mtgjson4.globals.VERSION_INFO)
        exit(0)

    # Ensure the language is a valid language
    if mtgjson4.globals.get_language_long_name(lang_to_process) is None:
        print('MTGJSON: Language \'{}\' not supported yet'.format(lang_to_process))
        exit(1)

    # Global of all sets to build
    SETS_TO_BUILD = determine_gatherer_sets(cl_args)

    # Start the build process
    start_time = time.time()

    card_loop = asyncio.get_event_loop()
    card_session = aiohttp.ClientSession(loop=card_loop, raise_for_status=True)
    card_loop.run_until_complete(main(card_loop, card_session, lang_to_process))

    if True:
        create_all_sets_files()

    end_time = time.time()
    print('Time: {}'.format(end_time - start_time))
