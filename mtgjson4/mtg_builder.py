import asyncio
import contextlib
import copy
import json
import os
import pathlib
import re
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import bs4

from mtgjson4 import mtg_corrections, mtg_global, mtg_http, mtg_parse, mtg_storage


class MTGJSON:
    def __init__(self,
                 sets_to_build: List[List[str]],
                 session: Optional[aiohttp.ClientSession] = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Start the class and define the i/o session and sets we'll have to build
        :param sets_to_build: List of Sets to build
        :param session: Aiohttp session. May be null.
        :param loop: Asyncio Loop. May be null.
        """
        if loop is None:
            loop = asyncio.events.get_event_loop()

        if session is None:
            session = aiohttp.ClientSession(
                loop=loop,
                raise_for_status=True,
                conn_timeout=60,
                read_timeout=60,
                connector=aiohttp.TCPConnector(limit=200))

        self.loop = loop
        self.http_session = session
        self.sets_to_build = sets_to_build

    async def get_card_html(self, card_mid: int, lookup_printed_text: bool = False) -> bs4.BeautifulSoup:
        """
        Gets the card details (first page) of a single card
        :param card_mid: Multiverse ID of requested card
        :param lookup_printed_text: Do we want the original text, or oracle text?
        :return: Returns a BeautifulSoup object of the requested gatherer page.
        """
        html = await mtg_http.get_card_details(self.http_session, card_mid, lookup_printed_text)
        soup = bs4.BeautifulSoup(html, 'html.parser')
        return soup

    @staticmethod
    def determine_layout_and_div_name(soup: bs4.BeautifulSoup,
                                      is_second_card: bool) -> Tuple[str, str, str, Optional[bool]]:
        """
        Determine the card's layout, which will then generate the parse div for all future operations
        Grab the ClientID from the page, which designates the front side's number (as it can be any int...)
        so we can have smarter parsing of the data
        """

        # Determine how many cards on on the page
        cards_total = len(soup.select('table[class^=cardDetails]'))

        number = soup.find_all('script')
        client_id_tags = ''
        for script in number:
            if 'ClientIDs' in script.get_text():
                client_id_tags = script.get_text()
                break

        # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{} for single cards or
        # ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_ctl0*_{} for double cards, * being any int
        div_name = str((client_id_tags.split('ClientIDs.nameRow = \'')[1].split(';')[0])[:-8] + "{}").strip()
        layout = 'unknown'
        add_additional_card = False
        second_div_name = div_name.replace('04', '05').replace('03', '04').replace('02', '03')

        if cards_total == 1:
            layout = 'normal'
        elif cards_total == 2:
            layout = 'double'
            if is_second_card:
                div_name = second_div_name
            else:
                add_additional_card = True

        return layout, div_name, second_div_name, add_additional_card

    async def build_foreign_info(self, soup: bs4.BeautifulSoup,
                                 second_card: bool) -> List[mtg_global.ForeignNamesDescription]:
        """
        Get the name and MID of this card for each other set it's printed in
        From there, we will get the foreign text/type for the user to decipher
        """
        language_rows = soup.select('table[class^=cardList]')[0]
        language_rows = language_rows.select('tr[class^=cardItem]')

        card_languages: List[mtg_global.ForeignNamesDescription] = list()
        for div in language_rows:
            table_rows = div.findAll('td')

            a_tag = table_rows[0].find('a')
            foreign_mid = a_tag['href'].split('=')[-1]
            card_language_mid = int(foreign_mid)
            card_foreign_name_in_language = a_tag.get_text(strip=True)

            card_language_name = table_rows[1].get_text(strip=True)

            # Download foreign URLs and append
            soup_print = await self.get_card_html(foreign_mid, True)

            # Determine if Card is Normal, Flip, or Split
            div_name = self.determine_layout_and_div_name(soup_print, second_card)[1]

            # Get Card Foreign Type
            c_foreign_type = mtg_parse.parse_card_types(soup_print, div_name)[3]

            # Following types are optionals, so we just build it here instead
            c_foreign_dict: mtg_global.ForeignNamesDescription = {
                'language': card_language_name,
                'name': card_foreign_name_in_language,
                'multiverseid': card_language_mid,
                'type': c_foreign_type,
                'text': None,
                'flavor': None
            }

            # Get Card Foreign Printed Rules Text
            c_foreign_text = mtg_parse.parse_text_and_color_identity(soup_print, div_name, None)[0]
            if c_foreign_text:
                c_foreign_dict['text'] = c_foreign_text

            # Get Card Foreign Flavor Text
            c_flavor_text = mtg_parse.parse_card_flavor(soup_print, div_name)
            if c_flavor_text:
                c_foreign_dict['flavor'] = c_flavor_text

            card_languages.append(c_foreign_dict)

        return card_languages

    async def build_main_part(self,
                              set_name: List[str],
                              card_mid: int,
                              other_cards_holder: Optional[List[object]],
                              second_card: bool = False) -> Dict[str, Any]:
        """
        This is the main builder for each card. This will put together the card, key by key, until it
        has most of its elements finished. There are some this doesn't encompass at this time, and those
        are handled by subsequent build functions.
        This builder only builds the main page of Gatherer.
        """

        # This will be the holder that is returned
        card_info = dict()

        # Parse web page so we can gather all data from it
        soup_oracle = await self.get_card_html(card_mid)

        card_layout, div_name, alt_div_name, add_other_card = self.determine_layout_and_div_name(
            soup_oracle, second_card)

        if add_other_card and (other_cards_holder is not None):
            other_card_mid: int = mtg_parse.parse_card_other_id(soup_oracle, alt_div_name)
            other_cards_holder.append(
                self.loop.create_task(self.build_card(set_name, other_card_mid, None, second_card=True)))

        card_info['multiverseid'] = int(str(card_mid))

        card_info['name'] = mtg_parse.parse_card_name(soup_oracle, div_name)
        card_info['convertedManaCost'] = mtg_parse.parse_card_cmc(soup_oracle, div_name)

        # Get other side's name for the user
        card_other_name = mtg_parse.parse_card_other_name(soup_oracle, alt_div_name, card_layout)
        if card_other_name is not None:
            if second_card:
                insert_order = [card_other_name, str(card_info['name'])]
            else:
                insert_order = [str(card_info['name']), card_other_name]

            card_info['names'] = insert_order

        # Get card's colors and mana cost
        card_colors, card_cost = mtg_parse.parse_colors_and_cost(soup_oracle, div_name)
        if card_colors:
            card_info['colors'] = card_colors
        if card_cost:
            card_info['manaCost'] = card_cost

        # Get Card Type(s)
        card_super_types, card_types, card_sub_types, full_type = mtg_parse.parse_card_types(soup_oracle, div_name)
        if card_super_types:
            card_info['supertypes'] = card_super_types
        if card_types:
            card_info['types'] = card_types
            if card_info['name'] in mtg_global.basic_lands:  # Basic lands (usually) have a watermark
                card_info['watermark'] = card_info['name']
        if card_sub_types:
            card_info['subtypes'] = card_sub_types
        if full_type:
            card_info['type'] = full_type

        # Get Card Text and Color Identity
        c_text, c_color_identity = mtg_parse.parse_text_and_color_identity(soup_oracle, div_name, card_colors)
        if c_text:
            if 'Planeswalker' in full_type:
                # Surround planeswalker activation cost by []
                # Ex: +1 => [+1]
                c_text = re.sub(r'([+−][0-9]):', r'[\1]:', c_text)

            card_info['text'] = c_text
        if c_color_identity:
            card_info['colorIdentity'] = c_color_identity

        # Get Card Flavor Text
        c_flavor = mtg_parse.parse_card_flavor(soup_oracle, div_name)
        if c_flavor:
            card_info['flavor'] = c_flavor

        # Get Card P/T OR Loyalty OR Hand/Life
        c_power, c_toughness, c_loyalty, c_hand, c_life = mtg_parse.parse_card_pt_loyalty_vanguard(
            soup_oracle, div_name)
        if c_power:
            card_info['power'] = c_power
        if c_toughness:
            card_info['toughness'] = c_toughness
        if c_loyalty:
            card_info['loyalty'] = c_loyalty
        if c_hand:
            card_info['hand'] = c_hand
        if c_life:
            card_info['life'] = c_life

        # Get Card Rarity
        card_info['rarity'] = mtg_parse.parse_card_rarity(soup_oracle, div_name)

        # Get Card Set Number
        c_number = mtg_parse.parse_card_number(soup_oracle, div_name)
        if c_number:
            card_info['number'] = c_number

        # Get Card Artist(s)
        card_info['artist'] = mtg_parse.parse_artists(soup_oracle, div_name)

        # Get Card Watermark
        c_watermark = mtg_parse.parse_watermark(soup_oracle, div_name)
        if c_watermark:
            card_info['watermark'] = c_watermark

        # Get Card Reserve List Status
        if card_info['name'] in mtg_global.RESERVE_LIST:
            card_info['reserved'] = True

        # Get Card Rulings
        c_rulings = mtg_parse.parse_rulings(soup_oracle, div_name)
        if c_rulings:
            card_info['rulings'] = c_rulings

        # Get Card Sets
        card_info['printings'] = mtg_parse.parse_card_sets(soup_oracle, div_name, set_name[1], self.sets_to_build)

        # Get Card Variations
        c_variations = mtg_parse.parse_card_variations(soup_oracle, div_name, int(str(card_mid)))
        if c_variations:
            card_info['variations'] = c_variations

        return card_info

    async def build_legalities_part(self, card_mid: int) -> Dict[str, Any]:
        """
        This builder will build from the legalities page of Gatherer
        """
        try:
            html = await mtg_http.get_card_legalities(self.http_session, card_mid)
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return dict()  # Page doesn't work, nothing we can do

            print("Unknown error: ", error.code)
            return dict()

        # Return holder
        card_info = dict()

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Legalities
        c_legal = mtg_parse.parse_card_legal(soup_oracle)
        if c_legal:
            card_info['legalities'] = c_legal

        return card_info

    async def build_foreign_part(self, card_mid: int, second_card: bool) -> Dict[str, Any]:
        """
        This builder builds the foreign identifiers page of gatherer
        """
        try:
            html = await mtg_http.get_card_foreign_details(self.http_session, card_mid)
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return dict()  # Page doesn't work, nothing we can do

            print("Unknown error: ", error.code)
            return dict()

        # Return holder
        card_info = dict()

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Foreign Data
        c_foreign_info = await self.build_foreign_info(soup_oracle, second_card)
        if c_foreign_info:
            card_info['foreignData'] = c_foreign_info

        return card_info

    async def build_original_details(self, card_mid: int, second_card: bool = False) -> Dict[str, Any]:
        """
        This builder builds the original type/text of the card. Useful for foreign languages
        and those who like the original printed text over Oracle text.
        """

        # Return holder
        card_info = dict()

        soup_print = await self.get_card_html(card_mid, True)

        # Determine if Card is Normal, Flip, or Split
        div_name = self.determine_layout_and_div_name(soup_print, second_card)[1]

        # Get Card Original Type
        c_original_type = mtg_parse.parse_card_types(soup_print, div_name)[3]

        if c_original_type:
            card_info['originalType'] = c_original_type

        # Get Card Original Text
        c_original_text = mtg_parse.parse_text_and_color_identity(soup_print, div_name, None)[0]
        if c_original_text:
            card_info['originalText'] = c_original_text

        return card_info

    @staticmethod
    def combine_all_parts_into_a_card(main_parts, original_parts, legal_parts, foreign_parts) -> Dict[str, Any]:
        """
        Will combine all dictionaries into one. If any conflicts arise, the newest value takes precedent
        """
        return {**main_parts, **original_parts, **legal_parts, **foreign_parts}

    async def build_card(self,
                         set_name: List[str],
                         card_mid: int,
                         other_cards_holder: Optional[List[object]],
                         second_card: bool = False) -> mtg_global.CardDescription:
        """
        This build method constructs the entire card from start to finish. It will call
        all the subsequent build methods, one by one, to put the card together.
        """
        main_parts = await self.build_main_part(set_name, card_mid, other_cards_holder, second_card=second_card)
        original_parts = await self.build_original_details(card_mid, second_card=second_card)
        legal_parts = await self.build_legalities_part(card_mid)
        foreign_parts = await self.build_foreign_part(card_mid, second_card=second_card)

        card_info: mtg_global.CardDescription = self.combine_all_parts_into_a_card(main_parts, original_parts,
                                                                                   legal_parts, foreign_parts)
        card_info['cardHash'] = mtg_parse.build_id_part(set_name, card_mid, card_info['name'])

        print('\tAdding {0} to {1}'.format(card_info['name'], set_name[0]))
        return card_info

    @staticmethod
    def rebuild_card_layouts(cards: List[mtg_global.CardDescription]) -> List[mtg_global.CardDescription]:
        """
        When we first build the card, the layout is only determining if it has one or two+ cards.
        As such, we now need to determine the real layout name and update the card's layout field.
        """

        def get_layout_from_other_side(c_info: mtg_global.CardDescription, unknown_num: int) -> str:
            """
            If the first side doesn't have enough information to determine the type,
            we can use the second side to help us out
            """
            try:
                card_2_name = next(card2 for card2 in c_info['names'] if c_info['name'] != card2)
            except StopIteration:
                return 'Double Card'
            card_2_info = next(card2 for card2 in cards if card2['name'] == card_2_name)
            if 'flip' in str(card_2_info['text']):
                return 'Flip'
            if 'transform' in str(card_2_info['text']):
                return 'Double-Faced'
            return f'Unknown{unknown_num}'

        return_cards = copy.copy(cards)
        for card_info in return_cards:
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
                if card_info['text'] is None:
                    card_layout = get_layout_from_other_side(card_info, 2)
                elif 'transform' in card_info['text']:
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
                    card_layout = get_layout_from_other_side(card_info, 1)
            else:
                card_layout = 'Meld'

            card_info['layout'] = card_layout
        return return_cards

    async def download_tokens_from_set(self, set_name: List[str]) -> List[mtg_global.TokenDescription]:
        """
        This will take the downloaded tokens (from the get all tokens method)
        and pull out the tokens necessary for the specific set being built
        """
        xml = await mtg_http.get_all_tokens(self.http_session)

        return_list: List[mtg_global.TokenDescription] = list()

        soup = bs4.BeautifulSoup(xml, 'html.parser')

        card_list = soup.find('cards').find_all('card')
        for card in card_list:
            printings = card.find_all('set')
            for printing in printings:
                if printing.get_text() == set_name[1]:
                    token_builder: mtg_global.TokenDescription = {
                        'name': None,
                        'colors': [],
                        'convertedManaCost': None,
                        'type': None,
                        'power': None,
                        'toughness': None,
                        'text': None,
                        'relatedToken': None,
                        'generators': []
                    }

                    with contextlib.suppress(AttributeError):
                        c_name = card.find('name').get_text()
                        token_builder['name'] = c_name

                    with contextlib.suppress(AttributeError):
                        c_colors = [c.get_text() for c in card.find_all('color')]
                        token_builder['colors'] = c_colors

                    with contextlib.suppress(AttributeError):
                        c_cmc = int(card.find('cmc').get_text())
                        token_builder['convertedManaCost'] = c_cmc

                    with contextlib.suppress(AttributeError):
                        c_type = card.find('type').get_text()
                        token_builder['type'] = c_type

                    with contextlib.suppress(AttributeError):
                        pt_find = card.find('pt').get_text().split('/')
                        c_power = pt_find[0]
                        c_toughness = pt_find[1]
                        token_builder['power'] = c_power
                        token_builder['toughness'] = c_toughness

                    with contextlib.suppress(AttributeError):
                        c_text = card.find('text').get_text()
                        token_builder['text'] = c_text

                    with contextlib.suppress(AttributeError):
                        c_related_token = card.find('related').get_text()
                        token_builder['relatedToken'] = c_related_token

                    with contextlib.suppress(AttributeError):
                        c_generators = [c.get_text() for c in card.find_all('reverse-related')]
                        token_builder['generators'] = c_generators

                    return_list.append(token_builder)

        return return_list

    async def download_cards_by_mid_list(self, set_name: List[str],
                                         multiverse_ids: List[int]) -> List[mtg_global.CardDescription]:
        """
        Method async calls the build process for each card in the multiverse_ids arg passed.
        There are two pass throughs; One for main cards, one for alternative sided cards
        that get added while the main process is running.
        """
        additional_cards: List[Any] = list()
        cards_in_set: List[mtg_global.CardDescription] = list()

        # start asyncio tasks for building each card
        futures = [
            self.loop.create_task(self.build_card(set_name, card_mid, additional_cards)) for card_mid in multiverse_ids
        ]

        # then wait until all of them are completed
        with contextlib.suppress(ValueError):  # Happens if no cards are in the multiverse_ids
            await asyncio.wait(futures)
            for future in futures:
                card_future = future.result()
                cards_in_set.append(card_future)

        with contextlib.suppress(ValueError):  # If no double-sided cards, gracefully skip
            await asyncio.wait(additional_cards)
            # print("Additional Cards found, lets work!")
            for future in additional_cards:
                card_future = future.result()
                cards_in_set.append(card_future)

        cards_in_set = self.rebuild_card_layouts(cards_in_set)
        # print(cards_in_set)

        return cards_in_set

    async def build_set(self, set_name: List[str]) -> Optional[Dict[str, Any]]:
        """
        Main method that will build the entire set by calling the build_*
        method(s) depending on what language(s) is/are required.
        :return: The entire set that was written to the file
        """

        async def get_mids_for_downloading() -> List[int]:
            """
            Use the URLs from another function to determine the MIDs that
            the build process should download and incorporate.
            """
            print('BuildSet: Building Set {}'.format(set_name[0]))

            # Cache Read
            if os.path.exists(os.path.join(mtg_storage.CACHE_DIR, 'set_checklists', set_name[1] + '.txt')):
                with mtg_storage.open_cache_location(f'set_checklists/{set_name[1]}.txt', 'r') as f:
                    urls_for_set = eval(f.read())
            else:
                # Cache Write
                urls_for_set = await mtg_http.get_checklist_urls(self.http_session, set_name)
                with mtg_storage.open_cache_location(f'set_checklists/{set_name[1]}.txt', 'w') as f:
                    f.write(str(urls_for_set))

            print('BuildSet: Acquired {1} URLs for {0}'.format(set_name[0], len(urls_for_set)))

            # ids_to_return = [235597]  # DEBUGGING IDs 235597,
            ids_to_return = await mtg_http.generate_mids_by_set(self.http_session, urls_for_set, set_name[1])
            return ids_to_return

        async def build_then_print_stuff(mids_for_set: List[int]) -> dict:
            """
            Function puts all sub-functions together to determine what IDs are
            needed for building, downloading the cards/building them, and
            applying the set configurations.
            :return: JSON text that was also written to file
            """

            set_stat = str(set_name[0])
            set_output = str(set_name[1])

            print('BuildSet: Determined {1} MIDs for {0}'.format(set_stat, len(mids_for_set)))
            cards_holder = await self.download_cards_by_mid_list(set_name, mids_for_set)

            print('BuildSet: Acquiring Tokens for {}'.format(set_stat))
            tokens_holder = await self.download_tokens_from_set(set_name)

            print('BuildSet: Applied Set Config options for {}'.format(set_stat))
            json_ready = await apply_set_config_options(set_name, cards_holder, tokens_holder)

            print('BuildSet: Generated JSON for {}'.format(set_stat))
            with mtg_storage.open_set_json(set_output, 'w') as f:
                json_ready = mtg_storage.remove_null_fields(json_ready)
                json.dump(json_ready, f, indent=4, sort_keys=True, ensure_ascii=False)
                print('BuildSet: JSON written for {0} ({1})'.format(set_stat, set_name[1]))

            return json_ready

        return await build_then_print_stuff(await get_mids_for_downloading())


async def apply_set_config_options(set_name: List[str], cards_dictionary: List[mtg_global.CardDescription],
                                   tokens_dictionary: List[mtg_global.TokenDescription]) -> Dict[str, Any]:
    """
    Take all options from set_config and add them to the final output product
    This will also add a version field, so it can be determined when it was last
    updated.
    """
    return_product = dict()

    # Will search the tree of set_configs to find the file
    with mtg_storage.open_set_config_json(set_name[1], 'r') as f:
        file_response = json.load(f)

    for key, value in file_response['SET'].items():
        return_product[key] = value

    # Declare the version of the build in the output file
    return_product['meta'] = {'version': mtg_global.__VERSION__, 'date': mtg_global.__VERSION_DATE__}

    if 'SET_CORRECTIONS' in file_response.keys():
        mtg_corrections.apply_corrections(file_response['SET_CORRECTIONS'], cards_dictionary)
    return_product['cards'] = cards_dictionary

    if tokens_dictionary:
        return_product['tokens'] = tokens_dictionary

    return return_product


def determine_gatherer_sets(args: Dict[str, Any]) -> List[List[str]]:
    """
    If the user wants a specific subset, get the set_configs for those specific sets.
    If they want all sets, pull every set_config file.
    Will exit if there is an invalid set code passed and alert for fixing.
    """

    def add_to_sets_to_build(root_p: str, file_p: str) -> None:
        """
        Append the set to the build process if it's real
        """
        with pathlib.Path(root_p, file_p).open('r', encoding='utf8') as f:
            this_set_name = json.load(f)
            if 'SET' in this_set_name:
                set_value = [this_set_name['SET']['name'], file.split('.json')[0]]
                all_sets.append(set_value)

    all_sets: List[List[str]] = list()
    if args['all_sets']:
        for root, _, files in os.walk(mtg_storage.SET_CONFIG_DIR):
            for file in files:
                if file.endswith('.json'):
                    add_to_sets_to_build(root, file)
    else:
        # Capitalize inputs and fix bad file names (WinOS can't have CON files, for example)
        set_args = [
            str(sa.upper() + '_') if sa.upper() in mtg_global.INVALID_FILE_NAMES else sa.upper() for sa in args['sets']
        ]

        for root, _, files in os.walk(mtg_storage.SET_CONFIG_DIR):
            for file in files:
                set_name: str = file.split('.json')[0]
                if str.upper(set_name) in set_args:
                    add_to_sets_to_build(root, file)
                    set_args.remove(set_name)

        # Ensure all sets provided by the user are valid
        if set_args:
            err = f"MTGJSON: Invalid Set Code(s) provided: {args['sets']}"
            print(err)
            raise ValueError(err)

    return all_sets


def create_combined_outputs() -> None:
    """
    This method class will create the combined output files
    like AllSets.json and AllCards.json
    """

    def create_all_sets() -> Dict[str, mtg_global.AllSetsDescription]:
        """
        This will create the AllSets.json file
        by pulling the compile data from the
        compiled sets and combining them into
        one conglomerate file.
        """
        all_sets_data: Dict[str, mtg_global.AllSetsDescription] = dict()
        for file in os.listdir(mtg_storage.SET_OUT_DIR):
            with pathlib.Path(mtg_storage.SET_OUT_DIR, file).open('r', encoding='utf-8') as f:
                file_content = json.load(f)

                # Do not add these to the final output
                file_content.pop('magicRaritiesCode', None)
                file_content.pop('essentialMagicCode', None)
                file_content.pop('useMagicRaritiesNumber', None)
                file_content.pop('code', None)
                file_content.pop('meta', None)
                file_content.pop('mkm_id', None)
                file_content.pop('mkm_name', None)
                file_content.pop('magicCardsInfoCode', None)

                set_name = file.split('.')[0]
                all_sets_data[set_name] = file_content
        return all_sets_data

    def create_all_cards() -> Dict[str, mtg_global.AllCardsDescription]:
        """
        This will create the AllCards.json file
        by pulling the compile data from the
        compiled sets and combining them into
        one conglomerate file.
        """
        all_cards_data: Dict[str, mtg_global.AllCardsDescription] = dict()
        for file in os.listdir(mtg_storage.SET_OUT_DIR):
            with pathlib.Path(mtg_storage.SET_OUT_DIR, file).open('r', encoding='utf-8') as f:
                file_content = json.load(f)

                # For each card in the set, attempt to add it
                for card in file_content['cards']:
                    if card['name'] not in all_cards_data.keys():

                        # Since these can vary from printing to printing
                        # We do not include them in the output
                        card.pop('artist', None)
                        card.pop('cardHash', None)
                        card.pop('flavor', None)
                        card.pop('multiverseid', None)
                        card.pop('number', None)
                        card.pop('originalText', None)
                        card.pop('originalType', None)
                        card.pop('rarity', None)
                        card.pop('variations', None)

                        all_cards_data[card['name']] = card
        return all_cards_data

    # Actual compilation process of the method
    all_sets = create_all_sets()
    all_cards = create_all_cards()

    mtg_storage.write_to_compiled_file('AllSets.json', all_sets)
    mtg_storage.write_to_compiled_file('AllCards.json', all_cards)
